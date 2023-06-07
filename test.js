import perspective from '@finos/perspective';
import * as aq from 'arquero';
import fs from 'node:fs';

let testData = null;

function generateData() {
  const dataEndTs = Date.now();
  const dataEntries = 10 * 24 * 60 * 60; // 1 month of data
  const tsIncrement = 1000; // entry for each 1 sec
  const dataStartTs = dataEndTs - tsIncrement * dataEntries;
  const data = [];
  let currTs = dataStartTs;
  for (let i = 0; i < dataEntries; i++) {
    const dataEntry = { ts: currTs, value: i };
    data.push(dataEntry);
    currTs += tsIncrement;
  }
  testData = data;
}

function formatBytes(a, b) {
  if (0 === a) return `0 `;
  let c = 1e3,
    d = b || 2,
    e = ['Bytes', 'KB', 'MB', 'GB'],
    f = Math.floor(Math.log(Math.abs(a)) / Math.log(c));
  return `${parseFloat((a / Math.pow(c, f)).toFixed(d))} ${e[f]}`;
}

function memDiff(info, memBefore, memAfter) {
  let msg = `Memory increase: ${info}\n`;
  msg += `  RSS      : ${formatBytes(memAfter.rss - memBefore.rss, 2)} (${memBefore.rss} -> ${memAfter.rss})\n`;
  msg += `  HeapTotal: ${formatBytes(memAfter.heapTotal - memBefore.heapTotal, 2)} (${memBefore.heapTotal} -> ${memAfter.heapTotal})\n`;
  msg += `  HeapUsed : ${formatBytes(memAfter.heapUsed - memBefore.heapUsed, 2)} (${memBefore.heapUsed} -> ${memAfter.heapUsed})\n`;
  msg += `  External : ${formatBytes(memAfter.external - memBefore.external, 2)} (${memBefore.external} -> ${memAfter.external})\n`;
  if ('arrayBuffers' in memBefore) {
    msg += `  arrayBuffers : ${formatBytes(memAfter.arrayBuffers - memBefore.arrayBuffers, 2)} (${memBefore.arrayBuffers} -> ${memAfter.arrayBuffers})\n`;
  }
  if ('wasmHeap' in memBefore) {
    msg += `  wasmHeap : ${formatBytes(memAfter.wasmHeap - memBefore.wasmHeap, 2)} (${memBefore.wasmHeap} -> ${memAfter.wasmHeap})\n`;
  }
  console.log(msg);
}

async function aggQuery(pTable) {
  const view = await pTable.view({
    columns: ['tb', 'value'],
    expressions: [`//tb\nbucket("ts", '1m')`],
    group_by: ['tb'],
    aggregates: { value: 'count' },
  });
  return view;
}

async function testPerspective() {
  console.log(`\nPerspective Table Test: data load from array`);
  global.gc();
  let startMemUsage = await perspective.memory_usage();
  let startTs = Date.now();
  const pTable = await perspective.table({ ts: 'datetime', value: 'integer' });
  await pTable.update(testData);
  let loadTs = Date.now();
  global.gc();
  let endMemUsage = await perspective.memory_usage();
  memDiff('Perspective data load from array', startMemUsage, endMemUsage);
  let numRows = await pTable.num_rows();
  console.log(`Table load time: ${loadTs - startTs} msec, rows: ${numRows}`);
  const qStartTs = Date.now();
  const qView = await aggQuery(pTable);
  const qEndTs = Date.now();
  const resJson = await qView.to_json();
  console.log(`Aggregation query time: ${qEndTs - qStartTs} msec, result rows: ${resJson.length - 1}`);
  await qView.delete();
  const v = await pTable.view();
  let arrowData = await v.to_arrow();
  fs.writeFileSync('dataset.arrow', Buffer.from(arrowData));
  arrowData = null;
  await v.delete();
  await pTable.delete();
  global.gc();
}

async function testPerspectiveFromArrow() {
  console.log(`\nPerspective Table Test: data load from Arrow dataset`);
  global.gc();
  let startMemUsage = await perspective.memory_usage();
  let startTs = Date.now();
  const arrowData = fs.readFileSync('dataset.arrow');
  const pTable = await perspective.table(arrowData);
  let loadTs = Date.now();
  global.gc();
  let endMemUsage = await perspective.memory_usage();
  memDiff('Perspective data load from Arrow dataset', startMemUsage, endMemUsage);
  let numRows = await pTable.num_rows();
  console.log(`Table load time: ${loadTs - startTs} msec, rows: ${numRows}`);
  const qStartTs = Date.now();
  const qView = await aggQuery(pTable);
  const qEndTs = Date.now();
  const resJson = await qView.to_json();
  console.log(`Aggregation query time: ${qEndTs - qStartTs} msec, result rows: ${resJson.length - 1}`);
  await qView.delete();
  await pTable.delete();
  global.gc();
}

async function testArquero() {
  console.log(`\nArquero Table Test: data load from array`);
  global.gc();
  let startMemUsage = process.memoryUsage();
  let startTs = Date.now();
  const arqTable = aq.from(testData);
  let loadTs = Date.now();
  global.gc();
  let endMemUsage = process.memoryUsage();
  memDiff('Arquero data load from array', startMemUsage, endMemUsage);
  let numRows = arqTable.numRows();
  console.log(`Table load time: ${loadTs - startTs} msec, rows: ${numRows}`);
  const qStartTs = Date.now();
  const qRes = arqTable
    .groupby({
      key: (d) => Math.floor(d.ts / 60000) * 60000,
    })
    .rollup({ mean: (d) => aq.op.count() });
  const qEndTs = Date.now();
  console.log(`Aggregation query time: ${qEndTs - qStartTs} msec, result rows: ${qRes.numRows()}`);
}

async function main() {
  generateData();
  await testPerspective();
  await testPerspectiveFromArrow();
  await testArquero();
}

main().catch((e) => {
  console.log(`Exception: ${e.message} ${e.stack}`);
  process.exit(-1);
});
