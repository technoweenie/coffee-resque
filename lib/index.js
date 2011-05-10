(function() {
  var Connection, EventEmitter, Worker, connectToRedis;
  var __slice = Array.prototype.slice, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  }, __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  exports.version = "0.1.3";
  exports.connect = function(options) {
    return new exports.Connection(options || {});
  };
  EventEmitter = require('events').EventEmitter;
  Connection = (function() {
    function Connection(options) {
      this.redis = options.redis || connectToRedis(options);
      this.namespace = options.namespace || 'resque';
      this.callbacks = options.callbacks || {};
      this.timeout = options.timeout || 5000;
      if (options.database != null) {
        this.redis.select(options.database);
      }
    }
    Connection.prototype.enqueue = function(queue, func, args) {
      this.redis.sadd(this.key('queues'), queue);
      return this.redis.rpush(this.key('queue', queue), JSON.stringify({
        "class": func,
        args: args || []
      }));
    };
    Connection.prototype.worker = function(queues, callbacks) {
      return new exports.Worker(this, queues, callbacks || this.callbacks);
    };
    Connection.prototype.end = function() {
      return this.redis.quit();
    };
    Connection.prototype.key = function() {
      var args;
      args = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
      args.unshift(this.namespace);
      return args.join(":");
    };
    return Connection;
  })();
  Worker = (function() {
    __extends(Worker, EventEmitter);
    function Worker(connection, queues, callbacks) {
      this.conn = connection;
      this.redis = connection.redis;
      this.queues = queues;
      this.callbacks = callbacks || {};
      this.running = false;
      this.ready = false;
      this.checkQueues();
    }
    Worker.prototype.start = function() {
      if (this.ready) {
        return this.init(__bind(function() {
          return this.poll();
        }, this));
      } else {
        return this.running = true;
      }
    };
    Worker.prototype.end = function(cb) {
      this.running = false;
      this.untrack();
      return this.redis.del([this.conn.key('worker', this.name, 'started'), this.conn.key('stat', 'failed', this.name), this.conn.key('stat', 'processed', this.name)], cb);
    };
    Worker.prototype.poll = function(title) {
      if (!this.running) {
        return;
      }
      if (title) {
        process.title = title;
      }
      this.queue = this.queues.shift();
      this.queues.push(this.queue);
      this.emit('poll', this, this.queue);
      return this.redis.lpop(this.conn.key('queue', this.queue), __bind(function(err, resp) {
        if (!err && resp) {
          return this.perform(JSON.parse(resp.toString()));
        } else {
          if (err) {
            this.emit('error', err, this, this.queue);
          }
          return this.pause();
        }
      }, this));
    };
    Worker.prototype.perform = function(job) {
      var cb, old_title;
      old_title = process.title;
      this.emit('job', this, this.queue, job);
      this.procline("" + this.queue + " job since " + ((new Date).toString()));
      if (cb = this.callbacks[job["class"]]) {
        return cb.apply(null, __slice.call(job.args).concat([__bind(function(result) {
          try {
            if (result instanceof Error) {
              return this.fail(result, job);
            } else {
              return this.succeed(result, job);
            }
          } finally {
            this.poll(old_title);
          }
        }, this)]));
      } else {
        this.fail(new Error("Missing Job: " + job["class"]), job);
        return this.poll(old_title);
      }
    };
    Worker.prototype.succeed = function(result, job) {
      this.redis.incr(this.conn.key('stat', 'processed'));
      this.redis.incr(this.conn.key('stat', 'processed', this.name));
      return this.emit('success', this, this.queue, job, result);
    };
    Worker.prototype.fail = function(err, job) {
      this.redis.incr(this.conn.key('stat', 'failed'));
      this.redis.incr(this.conn.key('stat', 'failed', this.name));
      this.redis.rpush(this.conn.key('failed'), JSON.stringify(this.failurePayload(err, job)));
      return this.emit('error', err, this, this.queue, job);
    };
    Worker.prototype.pause = function() {
      this.untrack();
      this.procline("Sleeping for " + (this.conn.timeout / 1000) + "s");
      return setTimeout(__bind(function() {
        if (!this.running) {
          return;
        }
        this.track();
        return this.poll();
      }, this), this.conn.timeout);
    };
    Worker.prototype.track = function() {
      this.running = true;
      return this.redis.sadd(this.conn.key('workers'), this.name);
    };
    Worker.prototype.untrack = function() {
      return this.redis.srem(this.conn.key('workers'), this.name);
    };
    Worker.prototype.init = function(cb) {
      var args, _ref;
      this.track();
      args = [this.conn.key('worker', this.name, 'started'), (new Date).toString()];
      this.procline("Processing " + this.queues.toString + " since " + args.last);
      if (cb) {
        args.push(cb);
      }
      return (_ref = this.redis).set.apply(_ref, args);
    };
    Worker.prototype.checkQueues = function() {
      if (this.queues.shift != null) {
        return;
      }
      if (this.queues === '*') {
        return this.redis.smembers(this.conn.key('queues'), __bind(function(err, resp) {
          this.queues = resp ? resp.sort() : [];
          this.ready = true;
          this.name = this._name;
          if (this.running) {
            return this.start();
          }
        }, this));
      } else {
        this.queues = this.queues.split(',');
        this.ready = true;
        return this.name = this._name;
      }
    };
    Worker.prototype.procline = function(msg) {
      return process.title = "resque-" + exports.version + ": " + msg;
    };
    Worker.prototype.failurePayload = function(err, job) {
      return {
        worker: this.name,
        queue: this.queue,
        payload: job,
        exception: 'Error',
        error: err.toString(),
        backtrace: err.stack.split('\n').slice(1),
        failed_at: (new Date).toString()
      };
    };
    Object.defineProperty(Worker.prototype, 'name', {
      get: function() {
        return this._name;
      },
      set: function(name) {
        return this._name = this.ready ? [name || 'node', process.pid, this.queues].join(":") : name;
      }
    });
    return Worker;
  })();
  connectToRedis = function(options) {
    var redis;
    redis = require('redis').createClient(options.port, options.host);
    if (options.password) {
      redis.auth(options.password);
    }
    return redis;
  };
  exports.Connection = Connection;
  exports.Worker = Worker;
}).call(this);
