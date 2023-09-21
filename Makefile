#! /usr/bin/make
BUILDTIME = $(shell date -u --rfc-3339=seconds)
GITHASH = $(shell git describe --dirty --always --tags)
GITCOMMITNO = $(shell git rev-list --all --count)
SHORTBUILDTAG = $(GITCOMMITNO).$(GITHASH)
BUILDINFO = Build Time:$(BUILDTIME)
LDFLAGS = -X 'main.buildTag=$(SHORTBUILDTAG)' -X 'main.buildInfo=$(BUILDINFO)'

TEST_AMQP_URI ?= amqp://guest:guest@localhost:5672/
COVERAGE_PATH ?= .coverage


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

bench:
	@TEST_AMQP_URI=$(TEST_AMQP_URI) go test -benchmem -run=^$ -v -count=2 -tags="rabbit" -bench .  ./...

coverage:
	@TEST_AMQP_URI=$(TEST_AMQP_URI) go test -covermode=count -tags="rabbit" -coverprofile=$(COVERAGE_PATH)

coverage-html:
	@rm $(COVERAGE_PATH) || true
	@$(MAKE) coverage
	@rm $(COVERAGE_PATH).html || true
	@go tool cover -html=$(COVERAGE_PATH) -o $(COVERAGE_PATH).html

coverage-browser:
	@rm $(COVERAGE_PATH) || true
	@$(MAKE) coverage
	@go tool cover -html=$(COVERAGE_PATH)

update-readme-badge:
	@go tool cover -func=$(COVERAGE_PATH) -o=$(COVERAGE_PATH).badge
	@go run github.com/AlexBeauchemin/gobadge@v0.3.0 -filename=$(COVERAGE_PATH).badge

# pkg.go.dev documentation is updated via go get
update-proxy-cache:
	@GOPROXY=https://proxy.golang.org go get github.com/danlock/rmq
