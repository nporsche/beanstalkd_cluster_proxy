target: dependency
	go build -o bin/talkteller main

debug: dependency
	go build -gcflags "-N -l" -o bin/talkteller main

GOPATH:=$(GOPATH):$(CURDIR)
export GOPATH

dependency:
	go get github.com/nporsche/goyaml
	go get github.com/nporsche/np-golang-pool
	go get github.com/nporsche/np-golang-logging

.PHONY: clean testall

clean:
	-rm -rf bin
	-rm -rf pkg

utest:
	go test util

itest:
	go test ./test
