const test = require('ava')

const setup = require('./setup.js')
const mongoDbQueue = require('../')
const queueName = 'enqueue'

let db

test.before('setup db', async t => {
  db = await setup(queueName) 
})

test('a single message enqueued should return a promise that resolves with an ObjectID', async t => {
  let q = await mongoDbQueue(db, queueName)
  let id = q.enqueue()
  t.is(id.constructor.name, 'Promise', 'returns a promise object')
  t.is(typeof id.then, 'function', 'returns a thanable')
  id.catch(t.fail)
  let id2 = await id
  t.is(id2.constructor.name, 'ObjectID')
})

test('multiple messages enqueued should return a promise that resolves with an Array<ObjectID>', async t => {
  let q = await mongoDbQueue(db, queueName)
  let idPromise = q.enqueue([{},{}])
    .catch(t.fail)
  t.is(idPromise.constructor.name, 'Promise', 'returns a promise object')
  t.is(typeof idPromise.then, 'function', 'returns a thanable')
  let id2 = await idPromise
  t.is(id2.constructor.name, 'Array')
  t.is(id2[0].constructor.name, 'ObjectID')
  t.is(id2[1].constructor.name, 'ObjectID')
  t.is(id2[2], undefined)
})

test('should accept empty payload', async t =>{
  let q = await mongoDbQueue(db, queueName)
  await t.notThrows(q.enqueue(null))
  await t.notThrows(q.dequeue(undefined))
  await t.notThrows(q.dequeue({}))
})

test.after('close the db conn', t => {
  db.close()
})