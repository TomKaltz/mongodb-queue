const test = require('ava')

const setup = require('./setup.js')
const mongoDbQueue = require('../')
const queueName = 'dequeue'

let db

test.before('setup db', async t => {
  db = await setup(queueName) 
})

test('should return a promise that resolves undefined if the queue is empty', async t => {
  let db2 = await setup('dequeue:empty')
  let q = await mongoDbQueue(db2, 'dequeue:empty')
  let msg1 = q.dequeue()
    .catch(t.fail)
  t.is(msg1.constructor.name, 'Promise', 'returns a promise object')
  t.is(typeof msg1.then, 'function', 'returns a thanable')
  let msg2 = await t.notThrows(msg1)
  t.is(msg2, undefined)
  db2.close()
})

test('should be tolerant to bad options passed in while queue is empty or not', async t =>{
  let q = await mongoDbQueue(db, queueName)
  await t.notThrows(q.dequeue(null))
  await t.notThrows(q.dequeue(undefined))
  await t.notThrows(q.dequeue({}))
  await t.notThrows(q.dequeue({lockDuration: undefined}))
  await t.notThrows(q.dequeue({lockDuration: null}))
  await t.notThrows(q.dequeue({lockDuration: 'bad data'}))
  //now add something to the queue
  let id = await q.enqueue({aribtrary: 'data'})
  t.is(id.constructor.name, 'ObjectID')
  await t.notThrows(q.dequeue(null))
  await t.notThrows(q.dequeue(undefined))
  await t.notThrows(q.dequeue({}))
  await t.notThrows(q.dequeue({lockDuration: undefined}))
  await t.notThrows(q.dequeue({lockDuration: null}))
  await t.notThrows(q.dequeue({lockDuration: 'bad data'}))
})

test.after('close the db conn', t => {
  db.close()
})