Consensus Server cluster
===

You should have received a detailed handout containing the
requirements of this project.  Please read it carefully.

Repository Layout
---

As usual, this repository is broken into several directories, only
some of which will be submitted as part of your solution.  They are:

* `api/`: This is given code that defines the various APIs required by
  this project.
* `cmd/`: These are commands, primarily intended for testing purposes.
  Several are provided for you.  You may create more commands and
  cause them to be built by editing the included Makefile
  appropriately.  Commands will be submitted with your project.
* `data/`: You will find data files here that can be used with your
  implementation.  In particular, configuration files used by the
  given server command are here.
* `given/`: This is given code, intended to provide functionality that
  is useful to you but which you need not implement yourself.
* `impl/`: There is some given code in this directory, but it is just
  skeleton code intended to help you get started.  This is where your
  implementation should go.  The entire `impl` directory will be
  submitted with your project.
* `tests/`: This directory is for tests that _use the published API_
  to test your project.  You may add more tests here, and you may wish
  to add tests in `impl` as well.  Your tests will be submitted with
  your project.

Testing
---

Minimal tests are provided in `tests`.  You will want to write
additional tests as you go.  For your convenience, the `make test`
project will run tests in `tests` and `impl`.  You may edit the
Makefile if you wish it to run tests in other packages.

Submission
---

Run `make submission` to create the file `raft.tar`.  Submit that file
to Autograder.
