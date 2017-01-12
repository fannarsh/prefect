'use strict';

// todo : graceful shutdown
// todo : handle kill signals
// todo : send messages to workers
// todo : forward messages from workers
// todo : minimize logging

var os = require('os');
var path = require('path');
var events = require('events');
var cluster = require('cluster');

var clusterSize = 0;
var listenersReady = false;
var rolling = false;
var retryCounter = 0;

var prefect = Object.create(events.EventEmitter.prototype);

function run (settings, config) {
  console.log('[prefect] run - settings:', settings);
  console.log('[prefect] run - config:', config);

  if (typeof settings === 'string') settings = { exec: settings };
  if (!settings.exec) throw new Error('Must define a exec script');

  var clusterSettings = {
    exec: path.resolve(settings.exec),
    silent: Boolean(settings.silent),
    // Send worker config as args to workers.
    args: [JSON.stringify(config || {})]
  };
  // todo : handle args, other then the worker-config

  cluster.setupMaster(clusterSettings);
  clusterSize = settings.size >= 0 ? settings.size : (os.cpus().length > 1 ? os.cpus().length - 1 : 1);

  registerListeners();
  startCluster();
}

function registerListeners () {
  if (listenersReady) return;

  cluster.on('online', function (worker) {
    console.log('[prefect][event:cluster-online] %s started', printWorker(worker));
  });

  cluster.on('exit', function (worker, code, signal) {
    if (signal) {
      console.log('[prefect][event:cluster-exit] %s was killed by signal: %s', printWorker(worker), signal);
    } else if (code !== 0) {
      console.log('[prefect][event:cluster-exit] %s exited with error code: %s', printWorker(worker), code);
    } else {
      console.log('[prefect][event:cluster-exit] %s exited', printWorker(worker));
    }
    restartDead();
  });
  listenersReady = true;
}

function printWorker (worker) {
  return 'worker(' + worker.id + '|' + worker.process.pid + ')';
}

function startCluster () {
  console.info('[prefect] Setting up cluster.');
  reroll();
}

function reroll () {
  if (rolling) return;
  rolling = true;
  var deprecatedWorkers = Object.keys(cluster.workers);
  var deprecatedSize = deprecatedWorkers.length;
  console.log('[prefect] Deprecated workers', deprecatedWorkers);
  next();

  function next () {
    var currentSize = Object.keys(cluster.workers).length;
    console.log(`[prefect] Rerolling..\n\tdeprecatedSize: ${deprecatedSize}\n\tcurrentSize: ${currentSize}\n\tclusterSize: ${clusterSize} `);

    if (deprecatedWorkers && deprecatedWorkers.length) {
      if (deprecatedWorkers.length <= clusterSize) return fork(roll, fail);
    }

    if (currentSize === clusterSize) {
      setTimeout(function () {
        rolling = false;
      }, 200);
      console.log('[prefect] Rerolling.. Finished');
      return prefect.emit('running', { old_size: deprecatedSize, current_size: currentSize });
    } else if (currentSize > clusterSize) {
      console.log('[prefect] Rerolling.. Need to shutdown some workers');
      roll();
    } else {
      console.log('[prefect] Rerolling.. Need more workers');
      fork(next, fail);
    }
  }

  // todo : betra check á workerid
  function roll () {
    var workerId = deprecatedWorkers.splice(0, 1)[0];
    console.log('[prefect]', workerId, deprecatedWorkers);
    if (!workerId) {
      return console.log('[prefect] Missing worker id!');
    }
    shutdown(workerId, next);
  }

  function fail () {
    console.log('[prefect] Failed starting worker');
    if (retryCounter < 3) {
      console.log('[prefect] Retry in a sec or two');
      setTimeout(function () {
        retryCounter++;
        rolling = false;
        reroll();
      }, 2000);
    } else {
      console.log('[prefect] Stopped retrying');
      setTimeout(function () {
        rolling = false;
      }, 200);
      retryCounter = 0;
    }
  }
}

function fork (success, fail) {
  console.info('[prefect] Forking a worker.');
  var worker = cluster.fork();

  worker.once('online', function () {
    console.log('[prefect][event:worker-online] online');
    var ifSuccessTimer = setTimeout(function () {
      console.log('[prefect][event:worker-online] success');
      worker.removeListener('exit', interning);
      if (success) success();
    }, 2000);
    worker.once('exit', interning);
    function interning () {
      console.log('[prefect][event:worker-exit] New worker died quickly. Aborting');
      clearTimeout(ifSuccessTimer);
      if (fail) process.nextTick(fail);
    }
  });
}

function shutdown (workerId, cb) {
  var worker = cluster.workers[workerId];
  if (!worker) {
    console.log('[prefect] Missing worker with id:', workerId);
    if (cb) cb(); // todo : err?
    return;
  }
  console.log('[prefect] Shutting down %s', printWorker(worker));
  if (cb) worker.on('disconnect', cb);
  worker.disconnect();
}

// todo : add some kind of retry rule, so that we don't continue to restart forever, which could happen sometimes.
function restartDead () {
  if (rolling) return;
  if (Object.keys(cluster.workers).length >= clusterSize) return;
  fork();
}

function sendConfig (config) {
  Object.keys(cluster.workers).forEach(function (key) {
    cluster.workers[key].send({ type: 'config', config: config });
  });
}

module.exports = prefect;
module.exports.run = run;
module.exports.sendConfig = sendConfig;
