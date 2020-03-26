# Copyright (c) 2020 Minoru Osuka
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 		http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GOOS ?=
GOARCH ?=
GO111MODULE ?= on
CGO_ENABLED ?= 0
CGO_CFLAGS ?=
CGO_LDFLAGS ?=
BUILD_TAGS ?=
VERSION ?=
BIN_EXT ?=
DOCKER_REPOSITORY ?= mosuka

PACKAGES = $(shell $(GO) list ./... | grep -v '/vendor/')

PROTOBUFS = $(shell find . -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq | grep -v /vendor/)

TARGET_PACKAGES = $(shell find . -name 'main.go' -print0 | xargs -0 -n1 dirname | sort | uniq | grep -v /vendor/)

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

.PHONY: protoc
protoc:
	@echo ">> generating proto3 code"
	@for proto_dir in $(PROTOBUFS); do echo $$proto_dir; protoc --proto_path=. --proto_path=$$proto_dir --proto_path=${GRPC_GATEWAY_PATH} --proto_path=${GRPC_GATEWAY_PATH}/third_party/googleapis --go_out=plugins=grpc:$(GOPATH)/src $$proto_dir/*.proto || exit 1; done
	@for proto_dir in $(PROTOBUFS); do echo $$proto_dir; protoc --proto_path=. --proto_path=$$proto_dir --proto_path=${GRPC_GATEWAY_PATH} --proto_path=${GRPC_GATEWAY_PATH}/third_party/googleapis --grpc-gateway_out=logtostderr=true,allow_delete_body=true:$(GOPATH)/src $$proto_dir/*.proto || exit 1; done

.PHONY: format
format:
	@echo ">> formatting code"
	@$(GO) fmt $(PACKAGES)

.PHONY: test
test:
	@echo ">> testing all packages"
	@echo "   GOOS        = $(GOOS)"
	@echo "   GOARCH      = $(GOARCH)"
	@echo "   CGO_ENABLED = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS  = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS  = $(BUILD_TAGS)"
	@$(GO) test -v -tags="$(BUILD_TAGS)" $(PACKAGES)

.PHONY: coverage
coverage:
	@echo ">> checking coverage of all packages"
	$(GO) test -coverprofile=./cover.out -tags="$(BUILD_TAGS)" $(PACKAGES)
	$(GO) tool cover -html=cover.out -o cover.html

.PHONY: build
build:
	@echo ">> building binaries"
	@echo "   GOOS        = $(GOOS)"
	@echo "   GOARCH      = $(GOARCH)"
	@echo "   CGO_ENABLED = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS  = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS  = $(BUILD_TAGS)"
	@echo "   VERSION     = $(VERSION)"
	@for target_pkg in $(TARGET_PACKAGES); do echo $$target_pkg; $(GO) build -tags="$(BUILD_TAGS)" $(LDFLAGS) -o ./bin/`basename $$target_pkg`$(BIN_EXT) $$target_pkg || exit 1; done

.PHONY: install
install:
	@echo ">> installing binaries"
	@echo "   GOOS        = $(GOOS)"
	@echo "   GOARCH      = $(GOARCH)"
	@echo "   CGO_ENABLED = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS  = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS  = $(BUILD_TAGS)"
	@echo "   VERSION     = $(VERSION)"
	@for target_pkg in $(TARGET_PACKAGES); do echo $$target_pkg; $(GO) install -tags="$(BUILD_TAGS)" $(LDFLAGS) $$target_pkg || exit 1; done

.PHONY: dist
dist:
	@echo ">> packaging binaries"
	@echo "   GOOS        = $(GOOS)"
	@echo "   GOARCH      = $(GOARCH)"
	@echo "   CGO_ENABLED = $(CGO_ENABLED)"
	@echo "   CGO_CFLAGS  = $(CGO_CFLAGS)"
	@echo "   CGO_LDFLAGS = $(CGO_LDFLAGS)"
	@echo "   BUILD_TAGS  = $(BUILD_TAGS)"
	@echo "   VERSION     = $(VERSION)"
	mkdir -p ./dist/$(GOOS)-$(GOARCH)/bin
	@for target_pkg in $(TARGET_PACKAGES); do echo $$target_pkg; $(GO) build -tags="$(BUILD_TAGS)" $(LDFLAGS) -o ./dist/$(GOOS)-$(GOARCH)/bin/`basename $$target_pkg`$(BIN_EXT) $$target_pkg || exit 1; done
	(cd ./dist/$(GOOS)-$(GOARCH); tar zcfv ../cete-${VERSION}.$(GOOS)-$(GOARCH).tar.gz .)

.PHONY: git-tag
git-tag:
	@echo ">> tagging github"
	@echo "   VERSION = $(VERSION)"
ifeq ($(VERSION),$(filter $(VERSION),latest master ""))
	@echo "please specify VERSION"
else
	git tag -a $(VERSION) -m "Release $(VERSION)"
	git push origin $(VERSION)
endif

.PHONY: docker-build
docker-build:
	@echo ">> building docker container image"
	@echo "   DOCKER_REPOSITORY = $(DOCKER_REPOSITORY)"
	@echo "   VERSION           = $(VERSION)"
	docker build -t $(DOCKER_REPOSITORY)/cete:latest --build-arg VERSION=$(VERSION) .
	docker tag $(DOCKER_REPOSITORY)/cete:latest $(DOCKER_REPOSITORY)/cete:$(VERSION)

.PHONY: docker-push
docker-push:
	@echo ">> pushing docker container image"
	@echo "   DOCKER_REPOSITORY = $(DOCKER_REPOSITORY)"
	@echo "   VERSION           = $(VERSION)"
	docker push $(DOCKER_REPOSITORY)/cete:latest
	docker push $(DOCKER_REPOSITORY)/cete:$(VERSION)

.PHONY: clean
clean:
	@echo ">> cleaning binaries"
	rm -rf ./bin
	rm -rf ./data
	rm -rf ./dist

.PHONY: cert
cert:
	@echo ">> generating certification"
	openssl req -x509 -nodes -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365
