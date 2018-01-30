const test = require('ava')

const setup = require('./setup.js')
const mongoDbQueue = require('../')
const queueName = 'default'

let db

test.before('setup db', async t => {
  db = await setup(queueName) 
})

test('fail if we do not provide a db connection', async t => {
  let err = await t.throws(mongoDbQueue(undefined, queueName))
  t.is(err.message, 'mongodb-job-queue: provide a mongodb.MongoClient')
})

test('fail if we do not provide a queue name', async t => {
  let err = await t.throws(mongoDbQueue(db))
  t.is(err.message, 'mongodb-job-queue: provide a queue name')
})

test('constructor returns a promise', async t => {
  let ret = mongoDbQueue(db, queueName)
  t.is(ret.constructor.name, 'Promise', 'returns a promise object')
  t.is(typeof ret.then, 'function', 'returns a thanable')
})

test('can create a valid queue object', async t => {
  let queue = await mongoDbQueue(db, queueName)
  t.truthy(queue, 'Queue created ok')
  t.is(queue.constructor.name, 'Queue')
  t.is(typeof queue.createIndexes, 'function')
  t.is(typeof queue.enqueue, 'function')
  t.is(typeof queue.dequeue, 'function')
  t.is(typeof queue.touch, 'function')
  t.is(typeof queue.ack, 'function')
  t.is(typeof queue.on, 'function')
  t.is(typeof queue.emit, 'function')
})

test('enqueue and then get a message', async t => {
  let q = await mongoDbQueue(db, queueName)
  let id = await q.enqueue({some: 'payload data'})
  t.is(id._bsontype, 'ObjectID', 'enqueue should return a bson ObjectID')
  let msg = await q.dequeue()
  t.deepEqual(msg.id, id, 'the dequeued message should have the same id as the enqueued message')
  let msg2 = await q.dequeue()
  t.is(msg2, undefined, 'upon second dequeue there should be no visisble messages')
})

test.after('close the db conn', t => {
  db.close()
})