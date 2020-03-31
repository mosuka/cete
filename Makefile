GOOS ?=
GOARCH ?=
GO111MODULE ?= on
CGO_ENABLED ?= 0
CGO_CFLAGS ?=
CGO_LDFLAGS ?=
BUILD_TAGS ?=
VERSION ?= $(shell git tag -l --sort=-v:refname | head -1)
BIN_EXT ?=
DOCKER_REPOSITORY ?= mosuka

PACKAGES = $(shell $(GO) list ./... | grep -v '/vendor/')

PROTOBUFS = $(shell find . -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq | grep -v /vendor/)

TARGET_PACKAGES = $(shell find $(CURDIR) -name 'main.go' -print0 | xargs -0 -n1 dirname | sort | uniq | grep -v /vendor/)

GRPC_GATEWAY_PATH = $(shell $(GO) list -m -f "{{.Dir}}" github.com/grpc-ecosystem/grpc-gateway)

ifeq ($(GOOS),)
  GOOS = $(shell go version | awk -F ' ' '{print $$NF}' | awk -F '/' '{print $$1}')
endif

ifeq ($(GOARCH),)
  GOARCH = $(shell go version | awk -F ' ' '{print $$NF}' | awk -F '/' '{print $$2}')
endif

ifeq ($(VERSION),)
  VERSION = latest
endif
LDFLAGS = -ldflags "-X \"github.com/mosuka/cete/version.Version=$(VERSION)\""

ifeq ($(GOOS),windows)
  BIN_EXT = .exe
endif

GO := GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) CGO_CFLAGS=$(CGO_CFLAGS) CGO_LDFLAGS=$(CGO_LDFLAGS) GO111MODULE=$(GO111MODULE) go

.DEFAULT_GOAL := build

.PHONY: showenv
showenv:
	@echo ">> show env"
	@echo "   GOOS              = $(GOOS)"
	@echo "   GOARCH            = $(GOARCH)"
	@echo "   GO111MODULE       = $(GO111MODULE)"
	@echo "   CGO_ENABLED       = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS        = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS       = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS        = $(BUILD_TAGS)"
	@echo "   VERSION           = $(VERSION)"
	@echo "   BIN_EXT           = $(BIN_EXT)"
	@echo "   DOCKER_REPOSITORY = $(DOCKER_REPOSITORY)"
	@echo "   LDFLAGS           = $(LDFLAGS)"
	@echo "   PACKAGES          = $(PACKAGES)"
	@echo "   PROTOBUFS         = $(PROTOBUFS)"
	@echo "   TARGET_PACKAGES   = $(TARGET_PACKAGES)"
	@echo "   GRPC_GATEWAY_PATH = $(GRPC_GATEWAY_PATH)"

.PHONY: protoc
protoc: showenv
	@echo ">> generating proto3 code"
	for proto_dir in $(PROTOBUFS); do echo $$proto_dir; protoc --proto_path=. --proto_path=$$proto_dir --proto_path=${GRPC_GATEWAY_PATH} --proto_path=${GRPC_GATEWAY_PATH}/third_party/googleapis --go_out=plugins=grpc:$(GOPATH)/src $$proto_dir/*.proto || exit 1; done
	for proto_dir in $(PROTOBUFS); do echo $$proto_dir; protoc --proto_path=. --proto_path=$$proto_dir --proto_path=${GRPC_GATEWAY_PATH} --proto_path=${GRPC_GATEWAY_PATH}/third_party/googleapis --grpc-gateway_out=logtostderr=true,allow_delete_body=true:$(GOPATH)/src $$proto_dir/*.proto || exit 1; done

.PHONY: format
format: showenv
	@echo ">> formatting code"
	$(GO) fmt $(PACKAGES)

.PHONY: test
test: showenv
	@echo ">> testing all packages"
	$(GO) test -v -tags="$(BUILD_TAGS)" $(PACKAGES)

.PHONY: coverage
coverage: showenv
	@echo ">> checking coverage of all packages"
	$(GO) test -coverprofile=./cover.out -tags="$(BUILD_TAGS)" $(PACKAGES)
	$(GO) tool cover -html=cover.out -o cover.html

.PHONY: clean
clean: showenv
	@echo ">> cleaning binaries"
	rm -rf ./bin
	rm -rf ./data
	rm -rf ./dist

.PHONY: build
build: showenv
	@echo ">> building binaries"
	for target_pkg in $(TARGET_PACKAGES); do echo $$target_pkg; $(GO) build -tags="$(BUILD_TAGS)" $(LDFLAGS) -o ./bin/`basename $$target_pkg`$(BIN_EXT) $$target_pkg || exit 1; done

.PHONY: install
install: showenv
	@echo ">> installing binaries"
	for target_pkg in $(TARGET_PACKAGES); do echo $$target_pkg; $(GO) install -tags="$(BUILD_TAGS)" $(LDFLAGS) $$target_pkg || exit 1; done

.PHONY: dist
dist: showenv
	@echo ">> packaging binaries"
	mkdir -p ./dist/$(GOOS)-$(GOARCH)/bin
	for target_pkg in $(TARGET_PACKAGES); do echo $$target_pkg; $(GO) build -tags="$(BUILD_TAGS)" $(LDFLAGS) -o ./dist/$(GOOS)-$(GOARCH)/bin/`basename $$target_pkg`$(BIN_EXT) $$target_pkg || exit 1; done
	(cd ./dist/$(GOOS)-$(GOARCH); tar zcfv ../cete-${VERSION}.$(GOOS)-$(GOARCH).tar.gz .)

.PHONY: tag
tag: showenv
	@echo ">> tagging github"
ifeq ($(VERSION),$(filter $(VERSION),latest master ""))
	@echo "please specify VERSION"
else
	git tag -a $(VERSION) -m "Release $(VERSION)"
	git push origin $(VERSION)
endif

.PHONY: docker-build
docker-build: showenv
	@echo ">> building docker container image"
	docker build -t $(DOCKER_REPOSITORY)/cete:latest --build-arg VERSION=$(VERSION) .
	docker tag $(DOCKER_REPOSITORY)/cete:latest $(DOCKER_REPOSITORY)/cete:$(VERSION)

.PHONY: docker-push
docker-push: showenv
	@echo ">> pushing docker container image"
	docker push $(DOCKER_REPOSITORY)/cete:latest
	docker push $(DOCKER_REPOSITORY)/cete:$(VERSION)

.PHONY: docker-clean
docker-clean: showenv
	docker rmi -f $(shell docker images --filter "dangling=true" -q --no-trunc)

.PHONY: cert
cert: showenv
	@echo ">> generating certification"
	openssl req -x509 -nodes -newkey rsa:4096 -keyout ./etc/cete-key.pem -out ./etc/cete-cert.pem -days 365 -subj '/CN=localhost'
