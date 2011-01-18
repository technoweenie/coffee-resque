# Coffee-Resque

Coffeescript/Node.js port of Resque.  

## INSTALLATION

1. Clone the repo (`git clone https://github.com/jashkenas/coffee-script.git`)
2. Generate the javascript (`make generate-js`)
3. Install with npm (`npm install` or `npm link`)

## USAGE

First, you'll want to queue some jobs in your app:

    var resque = require('coffee-resque').connect({
      host: redisHost, port: redisPort});
    resque.enqueue('math', 'add', [1,2])

Next, you'll want to setup a worker to handle these jobs.

    // implement your job functions
    var myJobs = {
      add: function(a, b) { a + b }
    }

    // setup a worker
    var worker = require('coffee-resque')
      .connect({host: redisHost, port: redisPort})
      .worker('*', myJobs)

    // some global event listeners
    //
    // Triggered every time the Worker polls.
    worker.on('poll', function(worker, queue) {})

    // Triggered before a Job is attempted.
    worker.on('job', function(worker, queue, job) {})

    // Triggered every time a Job errors.
    worker.on('error', function(err, worker, queue, job) {})

    // Triggered on every successful Job run.
    worker.on('success', function(worker, queue, job) {})

    worker.start()

## Development

All code is written in Coffee Script and converted to javascript as it's 
published to npm.

For normal development, all you need to be concerned about is testing:

  $ make test

If you need to generate javascript for production purposes and don't want to use npm packages, you can use:

  $ make generate-js
  $ make remove-js

You can also have coffeescript watch the src directory and generate javascript files as they're updated.

  $ make dev

## TODO

* Generic failure handling
* Better polling