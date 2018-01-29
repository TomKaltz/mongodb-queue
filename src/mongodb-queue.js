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

function nowPlusSecs(secs) {
  return new Date(Date.now() + secs * 1000)
}

module.exports = function(mongoDbClient, name, opts) {
  return new Queue(mongoDbClient, name, opts)
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
  this.queueId = `${os.hostname()}-${process.pid}`
  this.col = mongoDbClient.collection(name)
  this.ObjectID = require('mongodb-core').ObjectID
  this.lockDuration = opts.lockDuration || 10
  this.delay = opts.delay || 0
  this.maxRetries = opts.maxRetries || 5
  this.defaultPriority = opts.defaultPriority || 5

  this.createIndexes((err, indexname) => {
    if (err)
      throw new Error(
        `mongodb-job-queue: error creating index for queue: ${this.name}`
      )
  })
}

Queue.prototype.createIndexes = function(callback) {
  // TODO: probably index on status and visible
  // TODO: also make a separate index for waiting jobs
  var self = this

  self.col.createIndex({ status: 1, visible: 1 }, function(err, indexname) {
    if (err) return callback(err)
    callback(null, indexname)
    // self.col.createIndex({ ack : 1 }, { unique : true, sparse : true }, function(err) {
    //     if (err) return callback(err)
    //     callback(null, indexname)
    // })
  })
}

Queue.prototype.add = function(payload, opts, callback) {
  var self = this
  if (!callback) {
    callback = opts
    opts = {}
  }
  var delay = opts.delay || self.delay
  var visible = delay ? nowPlusSecs(delay) : new Date()
  var maxRetries = opts.maxRetries || self.maxRetries
  var priority = opts.priority || self.defaultPriority

  var msgs = []
  if (payload instanceof Array) {
    if (payload.length === 0) {
      return callback(
        new Error('Queue.add(): Array payload length must be greater than 0')
      )
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

  self.col.insertMany(msgs, function(err, results) {
    if (err) return callback(err)
    if (payload instanceof Array)
      return callback(null, results.insertedIds)
    return callback(null, results.ops[0]._id)
  })
}

Queue.prototype.get = function(opts, callback) {
  var self = this
  if (!callback) {
    callback = opts
    opts = {}
  }

  var visibility = opts.lockDuration || self.lockDuration
  var query = {
    status: 'waiting',
    visible: { $lte: new Date() },
  }
  var sort = {
    priority: -1,
    _id: 1,
  }
  var update = {
    $inc: { tries: 1 },
    $set: {
      status: 'running',
      visible: nowPlusSecs(visibility),
      lastStarted: new Date(),
      worker: self.queueId,
    },
  }

  self.col.findOneAndUpdate(
    query,
    update,
    { sort: sort, returnOriginal: false },
    function(err, result) {
      if (err) return callback(err)
      var msg = result.value
      if (!msg) return callback()

      // convert to an external representation
      msg = {
        // convert '_id' to an 'id' string
        id: msg._id,
        created: msg.created,
        status: msg.status,
        lockedUntil: msg.visible,
        payload: msg.payload,
        tries: msg.tries,
        maxRetries: msg.maxRetries,
        progress: msg.progress,
      }
      callback(null, msg)
    }
  )
}

Queue.prototype.touch = function(id, opts, callback) {
  var self = this
  if (!callback) {
    callback = opts
    opts = {}
  }

  var visibility = opts.lockDuration || self.lockDuration
  var query = {
    _id: id,
    status: 'running',
    visible: { $gt: new Date() },
  }
  var update = {
    $set: {
      visible: nowPlusSecs(visibility),
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
          `Queue.touch(): There is no running job with the id: ${id} or the job has expired.`
        )
      )
    }
    callback(null, msg.value._id)
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

Queue.prototype.done = function(id, callback) {
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
