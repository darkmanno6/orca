'use strict'
// Temporary CI probe: does input written to a fresh ConPTY pane under load get lost or delayed?
const { spawn: spawnChild } = require('node:child_process')
const os = require('node:os')
const pty = require('node-pty')

const PRELUDE_DA1 = '\x1b[c'
const PRELUDE_END = '\x1b[?9001h'
const DA1_REPLY = '\x1b[?1;2c'
const VARIANTS = (process.env.PROBE_VARIANTS || 'immediate,immediate-da1,after-prelude,conhost').split(',')
const BATCHES = Number(process.env.PROBE_BATCHES || 30)
const BATCH_SIZE = Number(process.env.PROBE_BATCH_SIZE || 8)
const FIRST_TIMEOUT_MS = 15_000
const SECOND_TIMEOUT_MS = 30_000
const MIRROR_STRESS = process.env.PROBE_MIRROR_STRESS === '1'
let survivor = null
let native = null

function log(obj) {
  process.stdout.write(`${JSON.stringify(obj)}\n`)
}

const burners = []
function startLoad() {
  const n = Number(process.env.PROBE_BURNERS ?? os.cpus().length)
  for (let i = 0; i < n; i += 1) {
    burners.push(
      spawnChild(process.execPath, ['-e', 'for(;;){}'], { stdio: 'ignore', windowsHide: true })
    )
  }
  return n
}

let seq = 0
function spawnPane(variant) {
  const id = seq++
  const t0 = performance.now()
  const rec = {
    id,
    variant,
    spawnMs: 0,
    workerReadyMs: null,
    socketConnectMs: null,
    firstDataMs: null,
    firstChunk: null,
    da1SeenMs: null,
    preludeEndMs: null,
    wroteMs: null,
    echoMs: null,
    outputMs: null,
    second: null,
    exited: false,
    output: ''
  }
  const proc = pty.spawn(process.env.ComSpec || 'cmd.exe', ['/d', '/q'], {
    cwd: process.cwd(),
    cols: 80,
    rows: 24,
    useConptyDll: variant !== 'conhost'
  })
  rec.spawnMs = performance.now() - t0
  rec.pid = proc.pid
  const agent = proc._agent
  agent?._conoutSocketWorker?.onReady(() => {
    rec.workerReadyMs = performance.now() - t0
  })
  agent?._outSocket?.on('connect', () => {
    rec.socketConnectMs = performance.now() - t0
  })
  const marker = `PROBE_${id}_OK`
  const typed = `echo ${marker.replace('OK', 'O^K')}\r`
  let wrote = false
  let da1Answered = false
  const write = () => {
    if (wrote) return
    wrote = true
    rec.wroteMs = performance.now() - t0
    proc.write(typed)
  }
  let resolveFirst
  rec.first = new Promise((r) => {
    resolveFirst = r
  })
  let resolveSecond = null
  let secondMarker = null
  proc.onExit(() => {
    rec.exited = true
    resolveFirst(false)
    resolveSecond?.(false)
  })
  proc.onData((chunk) => {
    const now = performance.now() - t0
    if (rec.firstDataMs === null) {
      rec.firstDataMs = now
      rec.firstChunk = JSON.stringify(chunk.slice(0, 40))
    }
    rec.output = (rec.output + chunk).slice(-4096)
    rec.da1Count = (rec.da1Count ?? 0) + chunk.split(PRELUDE_DA1).length - 1
    if (rec.killedAt !== undefined) rec.bytesAfterKill = (rec.bytesAfterKill ?? 0) + chunk.length
    if (rec.da1SeenMs === null && rec.output.includes(PRELUDE_DA1)) {
      rec.da1SeenMs = now
      if (variant === 'immediate-da1' || variant === 'after-prelude') {
        if (!da1Answered) {
          da1Answered = true
          proc.write(DA1_REPLY)
        }
      }
    }
    if (rec.preludeEndMs === null && rec.output.includes(PRELUDE_END)) {
      rec.preludeEndMs = now
      if (variant === 'after-prelude') write()
    }
    if (rec.echoMs === null && rec.output.includes(`echo ${marker.replace('OK', 'O^K')}`)) {
      rec.echoMs = now
    }
    if (rec.outputMs === null && rec.output.includes(marker)) {
      rec.outputMs = now
      resolveFirst(true)
    }
    if (secondMarker && rec.second && rec.second.outputMs === null && rec.output.includes(secondMarker)) {
      rec.second.outputMs = now
      resolveSecond(true)
    }
  })
  if (variant !== 'after-prelude') write()
  rec.writeSecond = () => {
    secondMarker = `PROBE_${id}_TWO`
    rec.second = { wroteMs: performance.now() - t0, outputMs: null }
    return new Promise((r) => {
      resolveSecond = r
      proc.write(`echo ${secondMarker.replace('TWO', 'T^WO')}\r`)
    })
  }
  rec.proc = proc
  return rec
}

const withTimeout = (p, ms) =>
  Promise.race([p, new Promise((r) => setTimeout(() => r('timeout'), ms))])

async function main() {
  if (MIRROR_STRESS) {
    const nativePath = require.resolve('node-pty/lib/utils')
    native = require(nativePath).loadNativeModule('conpty').module
    log({ phase: 'host-job', assigned: native.assignCurrentProcessToJob() })
    survivor = spawnPane('immediate')
    await survivor.first
  }
  log({ phase: 'start', cpus: os.cpus().length, burners: startLoad(), variants: VARIANTS, node: process.version })
  const all = []
  let previous = []
  for (let b = 0; b < BATCHES; b += 1) {
    const batch = []
    for (let i = 0; i < BATCH_SIZE; i += 1) {
      // Overlap teardown of the last batch with this batch's startup, like the stress test.
      if (previous[i]) {
        previous[i].killedAt = performance.now()
        previous[i].proc.kill()
      }
      batch.push(spawnPane(VARIANTS[(b * BATCH_SIZE + i) % VARIANTS.length]))
      if (survivor) {
        survivor.proc.resize(80 + (b % 2), 24)
        survivor.proc.clear()
        native.listJobProcessIds(survivor.proc._pty, survivor.proc.pid)
      }
    }
    await Promise.all(
      batch.map(async (rec) => {
        const first = await withTimeout(rec.first, FIRST_TIMEOUT_MS)
        rec.firstResult = first === true ? 'ok' : first === false ? 'exited' : 'timeout'
        if (rec.firstResult !== 'ok') {
          // Delay vs loss: keep waiting on the first marker and probe whether input works at all.
          const second = await withTimeout(rec.writeSecond(), SECOND_TIMEOUT_MS)
          rec.secondResult = second === true ? 'ok' : second === false ? 'exited' : 'timeout'
        }
        const { proc, first: _f, writeSecond: _w, output, ...rest } = rec
        const bad = rec.firstResult !== 'ok'
        log({
          phase: 'pane',
          batch: b,
          ...rest,
          preludeSeen: rec.da1SeenMs !== null,
          ...(bad || rec.da1SeenMs === null ? { output } : {})
        })
        all.push(rec)
      })
    )
    // Report any killed pane that kept receiving bytes, or saw more than one prelude.
    for (const rec of previous) {
      if (rec.bytesAfterKill || rec.da1Count > 1) {
        log({ phase: 'crosstalk', id: rec.id, bytesAfterKill: rec.bytesAfterKill, da1Count: rec.da1Count, tail: JSON.stringify(rec.output.slice(-300)) })
      }
    }
    previous = batch
  }
  for (const rec of previous) rec.proc.kill()
  for (const burner of burners) burner.kill()
  const summary = {}
  for (const rec of all) {
    const s = (summary[rec.variant] ??= {
      panes: 0,
      firstTimeout: 0,
      firstDelayedOver3s: 0,
      noPrelude: 0,
      noPreludeAndTimeout: 0,
      secondOkAfterTimeout: 0,
      lateFirstAfterTimeout: 0,
      maxOutputMs: 0
    })
    s.panes += 1
    if (rec.firstResult !== 'ok') s.firstTimeout += 1
    if (rec.outputMs !== null && rec.outputMs - rec.wroteMs > 3000) s.firstDelayedOver3s += 1
    if (rec.da1SeenMs === null && rec.variant !== 'conhost') s.noPrelude += 1
    if (rec.da1SeenMs === null && rec.firstResult !== 'ok') s.noPreludeAndTimeout += 1
    if (rec.secondResult === 'ok') s.secondOkAfterTimeout += 1
    if (rec.firstResult !== 'ok' && rec.outputMs !== null) s.lateFirstAfterTimeout += 1
    if (rec.outputMs !== null) s.maxOutputMs = Math.max(s.maxOutputMs, Math.round(rec.outputMs))
  }
  log({ phase: 'summary', summary })
  setTimeout(() => process.exit(0), 3000)
}

main().catch((error) => {
  log({ phase: 'error', message: String(error?.stack || error) })
  for (const burner of burners) burner.kill()
  process.exit(0)
})
