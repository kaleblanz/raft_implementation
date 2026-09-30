GO := go
PROTOC := protoc

# Build the following commands.  This assumes that each command
# CMDNAME is in the directory cmd/CMDNAME, and can be built by
# changing to that directory and running go build.
COMMANDS := server client_set client_get endlessElectionsWinner

# These commands are tests provided in the given code
TESTCOMMANDS := checkconfig

# This rule turns COMMANDS into executable filenames, do not change.
# You don't need to understand this.
CMDFILES := $(shell for word in $(COMMANDS) $(TESTCOMMANDS); do echo cmd/$$word/$$word; done)

# Running the command `make` with no arguments should build the
# commands specified in $(COMMANDS).  This is for your convenience,
# you can also build them with `go build`.
#
# The body of this rule is a shell script that loops through every
# command defined in COMMANDS and builds it with go build.  Shell
# scripts embedded in Makefiles have somewhat strange parsing rules
# due to the way that Make works; see `info make` for more
# information.
all: go.sum
	@for cmd in $(COMMANDS); do                             \
	    echo "Building $$cmd";                              \
	    (cd cmd/$$cmd; go build);                           \
        done

submission:
	tar cf raft.tar \
	    $(shell for word in $(CMDFILES); do echo "--exclude $$word"; done) \
	    --exclude '.*' --exclude '.DS_Store' \
	    Makefile impl tests cmd

giventest: api/messages.pb.go
	@for cmd in checkconfig client_set; do \
	    echo "Building $$cmd";             \
	    (cd cmd/$$cmd && go build);        \
	done
	@echo "Checking given configurations"
	@for file in data/*.json; do                                        \
	    cmd/checkconfig/checkconfig $$file || echo "$$file is invalid"; \
	done
	@echo "Running go test"
	go test cse586.raftm/api

go.sum: api/messages.pb.go
	go get cse586.raftm/api

# If you don't want to wait for the binaries to build every time you
# run make test, you can remove "all" from the following line, or
# replace it with only those binaries needed in your testing.
test: api/messages.pb.go all
	go test cse586.raftm/tests
	go test cse586.raftm/impl

clean:
	rm -f $(CMDFILES) raftm.tar
	find . -name '*~' -delete

# Build a protobuf implementation from a protocol description
build-protobuf:
	$(PROTOC) --go_out=. --go_opt=paths=source_relative api/messages.proto

.PHONY: all clean submission giventest test
