# Coffee-Resque

Coffeescript/Node.js port of Resque.  

## USAGE

First, you'll want to queue some jobs in your app:

    var resque = require('resque').connect({
      host: redisHost, port: redisPort});
    resque.enqueue('myJobs.add', 'add', [1,2])

Next, create a job.  A job is just a function that takes a callback at the end.

    // myJobs.js
    exports.add = function(next, a, b) {
      try {
        a + b
        next()
      catch(err) {
        next(err)
      }
    }

Finally, you'll want to setup a worker to handle these jobs.

    // get my jobs
    var myJobs = require('myJobs')

    // setup a worker
    var resque = require('resque')
      .connect({host: redisHost, port: redisPort})

    // bind the myJobs.add function from above
    resque.job('myJobs.add', myJobs.add)

    // add an ad-hoc job
    resque.job('myJobs.subtract', function(next, a, b) {
      try {
        a - b
        next()
      catch(err) {
        next(err)
      }
    })

    // Triggered before a Job is attempted.
    resque.on('job', function(queue, job) {})

    // Triggered every time a Job errors.
    resque.on('error', function(err, queue, job) {})

    // Triggered on every successful Job run.
    resque.on('success', function(queue, job) {})

    resque.poll('*')

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