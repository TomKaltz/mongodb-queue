var async = require('async')
var test = require('tape')

var setup = require('./setup.js')
var mongoDbQueue = require('../')

setup(function(db) {
  test('first test', function(t) {
    var queue = mongoDbQueue(db, 'default')
    t.ok(queue, 'Queue created ok')
    t.end()
  })

  test('single round trip', function(t) {
    var queue = mongoDbQueue(db, 'default')
    var msg

    t.test('add message', function(t) {
      queue.add({text:'Hello, World!'}, function(err, id) {
        t.ok(!err, 'There is no error when adding a message.')
        t.ok(id, 'Received an id for this message')
        t.end()
      })
    })
    t.test('retrieve a message', function(t) {
      queue.get(function(err, thisMsg) {
        msg = thisMsg
        t.ok(msg.id, 'Got a msg.id')
        // t.equal(typeof msg.id, 'string', 'msg.id is a string')
        // t.ok(msg.ack, 'Got a msg.ack'1
        // t.equal(typeof msg.ack, 'string', 'msg.ack is a string')
        t.ok(msg.tries, 'Got a msg.tries')
        t.equal(typeof msg.tries, 'number', 'msg.tries is a number')
        t.equal(msg.tries, 1, 'msg.tries is currently one')
        t.deepEqual(msg.payload, {text:'Hello, World!'}, 'Payload is correct')
        t.equal(msg.status, 'running', 'Job has running status')
        t.end()
      })
    })
    t.test('finish a message', function(t) {
      queue.done(msg.id, function(err, id) {
        t.ok(!err, 'No error when acking the message')
        t.ok(id, 'Received an id when acking this message')
        t.end()
      })
    })
    t.end()
    //   ],
    //   function(err) {
    //     t.ok(!err, 'No error during single round-trip test')
    //     t.end()
    //   }
    // )
  })

  test('single round trip, can\'t be acked again', function(t) {
    var queue = mongoDbQueue(db, 'default')
    var msg

    async.series(
      [
        function(next) {
          queue.add({text:'Hello, World!'}, function(err, id) {
            t.ok(!err, 'There is no error when adding a message.')
            t.ok(id, 'Received an id for this message')
            next()
          })
        },
        function(next) {
          queue.get(function(err, thisMsg) {
            msg = thisMsg
            t.ok(msg.id, 'Got a msg.id')
            t.equal(typeof msg.id, 'object', 'msg.id is a string')
            t.ok(msg.tries, 'Got a msg.tries')
            t.equal(typeof msg.tries, 'number', 'msg.tries is a number')
            t.equal(msg.tries, 1, 'msg.tries is currently one')
            t.deepEqual(msg.payload, {text:'Hello, World!'}, 'Payload is correct')
            next()
          })
        },
        function(next) {
          queue.done(msg.id, function(err, id) {
            t.ok(!err, 'No error when acking the message')
            t.ok(id, 'Received an id when acking this message')
            next()
          })
        },
        function(next) {
          queue.done(msg.id, function(err, id) {
            t.ok(err, 'There is an error when acking the message again')
            t.ok(!id, 'No id received when trying to ack an already deleted message')
            t.equal(id, undefined)
            next()
          })
        },
      ],
      function(err) {
        t.ok(!err, 'No error during single round-trip when trying to double ack')
        t.end()
      }
    )
  })

  test('db.close()', function(t) {
    t.pass('db.close()')
    db.close()
    t.end()
  })

})
