#! /usr/bin/make
BUILDTIME = $(shell date -u --rfc-3339=seconds)
GITHASH = $(shell git describe --dirty --always --tags)
GITCOMMITNO = $(shell git rev-list --all --count)
SHORTBUILDTAG = $(GITCOMMITNO).$(GITHASH)
BUILDINFO = Build Time:$(BUILDTIME)
LDFLAGS = -X 'main.buildTag=$(SHORTBUILDTAG)' -X 'main.buildInfo=$(BUILDINFO)'

TEST_AMQP_URI ?= amqp://guest:guest@localhost:5672/

depend: deps
deps:
	go get ./...
	go mod tidy

version:
	@echo $(SHORTBUILDTAG)

unit-test:
	@go test -race -count=3 ./...

test:
	@TEST_AMQP_URI=$(TEST_AMQP_URI) go test -v -race -count=2 -tags="rabbit" ./...

coverage:
	@TEST_AMQP_URI=$(TEST_AMQP_URI) go test -v -race -count=2 -tags="rabbit" -coverprofile=./.coverage  ./...

coverage-html:
	@rm ./.coverage || true
	@$(MAKE) coverage
	@rm ./.coverage.html || true
	@go tool cover -html=./.coverage -o ./.coverage.html

coverage-browser:
	@rm ./.coverage || true
	@$(MAKE) coverage
	@go tool cover -html=./.coverage
