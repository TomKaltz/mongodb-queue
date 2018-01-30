/**
 *
 * mongodb-queue.js - Use your existing MongoDB as a local queue.
 *
 * Copyright (c) 2014 Andrew Chilton
 * - http://chilts.org/
 * - andychilton@gmail.com
 *
 * License: http://chilts.mit-license.org/2014/
 *
 **/
const os = require('os')
const util = require('util')

function nowPlusSecs(secs) {
  return new Date(Date.now() + secs * 1000)
}

module.exports = async function(mongoDbClient, name, opts) {
  let q = new Queue(mongoDbClient, name, opts)  
  try {
    await q.createIndexes()
    return q
  } catch (error) {
    throw new Error(`mongodb-job-queue: error creating index for queue: ${q.name}`)
  }
}

// the Queue object itself
function Queue(mongoDbClient, name, opts) {
  if (!mongoDbClient) {
    throw new Error('mongodb-job-queue: provide a mongodb.MongoClient')
  }
  if (!name) {
    throw new Error('mongodb-job-queue: provide a queue name')
  }
  opts = opts || {}
  this.name = name
  this.workerId = `${os.hostname()}-${process.pid}`
  this.col = mongoDbClient.collection(name)
  this.lockDuration = opts.lockDuration || 10
  this.delay = opts.delay || 0
  this.maxRetries = opts.maxRetries || 5
  this.priority = opts.priority || 5
  return this
}

let EventEmitter = require('events').EventEmitter
util.inherits(Queue, EventEmitter)

Queue.prototype.createIndexes = async function() {
  // TODO: probably index on status and visible
  // TODO: also make a separate index for waiting jobs
  var self = this
  return new Promise((resolve, reject)=>{
    self.col.createIndex({ status: 1, visible: 1 }, function(err, indexname) {
      if (err) return reject(err)
      resolve(indexname)
      // self.col.createIndex({ ack : 1 }, { unique : true, sparse : true }, function(err) {
      //     if (err) return callback(err)
      //     callback(null, indexname)
      // })
    })
  })
}

Queue.prototype.enqueue = async function(payload, opts = {}) {
  let self = this
  let delay = opts.delay || self.delay
  let visible = delay ? nowPlusSecs(delay) : new Date()
  let maxRetries = opts.maxRetries || self.maxRetries
  let priority = opts.priority || self.priority

  var msgs = []
  if (payload instanceof Array) {
    if (payload.length === 0) {
      throw new Error('Queue.add(): Array payload length must be greater than 0')
    }
    payload.forEach(function(payload) {
      msgs.push({
        created: new Date(),
        status: 'waiting',
        priority,
        visible,
        payload,
        tries: 0,
        maxRetries,
        progress: 0,
        lastStarted: null,
        lastFailed: null,
        lastFailReason: null,
        worker: null,
      })
    })
  } else {
    msgs.push({
      created: new Date(),
      status: 'waiting',
      priority,
      visible,
      payload,
      tries: 0,
      maxRetries,
      progress: 0,
      lastStarted: null,
      lastFailed: null,
      lastFailReason: null,
      worker: null,
    })
  }
  return new Promise((resolve, reject) => {
    self.col.insertMany(msgs, function(err, results) {
      if (err) return reject(err)
      if (payload instanceof Array)
        return resolve(results.insertedIds)
      return resolve(results.ops[0]._id)
    })
  })
}

Queue.prototype.dequeue = async function(opts = {}) {
  let self = this
  if(!opts) opts = {}
  let visibility = opts.lockDuration || self.lockDuration
  let query = {
    status: 'waiting',
    visible: { $lte: new Date() },
  }
  let sort = {
    priority: -1,
    _id: 1,
  }
  let update = {
    $inc: { tries: 1 },
    $set: {
      status: 'running',
      visible: nowPlusSecs(visibility),
      lastStarted: new Date(),
      worker: self.workerId,
    },
  }
  return new Promise((resolve, reject)=>{
    self.col.findOneAndUpdate(
      query,
      update,
      { sort: sort, returnOriginal: false },
      function(err, result) {
        if (err) return reject(err)
        var msg = result.value
        if (!msg) return resolve(undefined)
  
        // convert to an external representation
        msg = {
          id: msg._id,
          created: msg.created,
          status: msg.status,
          lockedUntil: msg.visible,
          payload: msg.payload,
          tries: msg.tries,
          maxRetries: msg.maxRetries,
          progress: msg.progress,
        }
        resolve(msg)
      }
    )
  })
}

Queue.prototype.touch = async function(id, opts = {}) {
  let self = this
  
  let visibility = opts.lockDuration || self.lockDuration
  let query = {
    _id: id,
    status: 'running',
    visible: { $gt: new Date() },
  }
  let update = {
    $set: {
      visible: nowPlusSecs(visibility),
    },
  }

  return new Promise((resolve, reject)=>{
    self.col.updateOne(query, update, { new: true }, function(err, res) {
      if (err) return reject(err)
      if (res.modifiedCount !== 1) {
        return reject(new Error(`Queue.touch(): There is no running job with the id: ${id} or the job has expired.`))
      }
      return resolve(true)
    })
  })
}

Queue.prototype.ack = function(id, callback) {
  var self = this
  var query = {
    _id: id,
    visible: { $gt: new Date() },
    status: 'running',
  }
  var update = {
    $set: {
      status: 'success',
      //worker: null,
      progress: 100,
    },
  }
  self.col.findOneAndUpdate(query, update, { returnOriginal: false }, function(
    err,
    msg
  ) {
    if (err) return callback(err)
    if (!msg.value) {
      return callback(
        new Error(
          `Queue.done(): There is no job with the id: ${id} or the job has already finished.`
        )
      )
    }
    callback(null, '' + msg.value._id)
  })
}

Queue.prototype.progress = function(id, progress, callback) {
  let self = this

  var query = {
    _id: id,
    status: 'running',
    visible: { $gt: new Date() },
  }
  if (progress < 0) progress = 0
  if (progress > 100) progress = 100
  var update = {
    $set: {
      progress,
    },
  }
  self.col.findOneAndUpdate(query, update, { returnOriginal: false }, function(
    err,
    msg
  ) {
    if (err) return callback(err)
    if (!msg.value) {
      return callback(
        new Error(
          `Queue.progress(): There is no running job with the id: ${id} or the job has expired.`
        )
      )
    }
    callback(null, msg.value._id)
  })
}

Queue.prototype.fail = function(id, reason, callback) {
  var self = this

  var query = {
    _id: id,
    visible: { $gt: new Date() },
    status: 'running',
  }
  var update = {
    $set: {
      //worker: null,
      lastFailed: new Date(),
      lastFailReason: reason,
    },
  }
  self.col.findOne(query, { returnOriginal: false }, function(err, msg) {
    if (err) return callback(err)
    if (!msg.value) {
      return callback(
        new Error(
          `Queue.done(): There is no job with the id: ${id} or the job has already finished.`
        )
      )
    }
    if (msg.tries > msg.maxRetires) {
      update.status = 'failed'
    } else {
      (update.status = 'waiting'), (update.visible = new Date())
    }
    self.col.findOneAndUpdate(
      query,
      update,
      { returnOriginal: false },
      function(err, msg) {
        if (err) return callback(err)
        if (!msg.value) {
          return callback(
            new Error(
              `Queue.done(): There is no job with the id: ${id} or the job has already finished.`
            )
          )
        }
        callback(null, '' + msg.value._id)
      }
    )
  })
}

Queue.prototype.failStaleJobs = async function(callback) {
//   var retryQuery = {
//     visible: { $lte: new Date() },
//     status: 'running',
//   }
//   var retryUpdate = {
//   }
//   return await this.col.updateMany(retryQuery, retryUpdate)
//    
  // if (msg.tries > msg.maxRetires) {
//     //   update.status = 'failed'
//     // } else {
//     //   (update.status = 'waiting'), (update.visible = new Date())
//     // }
}


Queue.prototype.clean = function(callback) {
  var self = this

  var query = {
    status: { $not: 'running' },
  }
  self.col.deleteMany(query, callback)
}
Queue.prototype.total = function(callback) {
  var self = this

  self.col.count(function(err, count) {
    if (err) return callback(err)
    callback(null, count)
  })
}

Queue.prototype.waiting = function(callback) {
  var self = this

  var query = {
    status: 'waiting',
    visible: { $lte: new Date() },
  }

  self.col.count(query, function(err, count) {
    if (err) return callback(err)
    callback(null, count)
  })
}

Queue.prototype.inFlight = function(callback) {
  var self = this

  var query = {
    status: 'running',
  }

  self.col.count(query, function(err, count) {
    if (err) return callback(err)
    callback(null, count)
  })
}

Queue.prototype.succeeded = function(callback) {
  var self = this

  var query = {
    status: 'success',
  }

  self.col.count(query, function(err, count) {
    if (err) return callback(err)
    callback(null, count)
  })
}

Queue.prototype.failed = function(callback) {
  var self = this

  var query = {
    status: 'failed',
  }

  self.col.count(query, function(err, count) {
    if (err) return callback(err)
    callback(null, count)
  })
}

Queue.prototype.cancelled = function(callback) {
  var self = this

  var query = {
    status: 'cancelled',
  }

  self.col.count(query, function(err, count) {
    if (err) return callback(err)
    callback(null, count)
  })
}
