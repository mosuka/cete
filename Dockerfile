# Copyright (c) 2019 Minoru Osuka
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

FROM golang:1.12.1-alpine3.9

ARG VERSION

ENV GOPATH /go

COPY . ${GOPATH}/src/github.com/mosuka/cete

RUN apk update && \
    apk upgrade && \
    apk add git && \
    apk add make && \
    cd ${GOPATH}/src/github.com/mosuka/cete && \
    make \
      GOOS=linux \
      GOARCH=amd64 \
      CGO_ENABLED=0 \
      VERSION="${VERSION}" \
      build

FROM alpine:3.9

MAINTAINER Minoru Osuka "minoru.osuka@gmail.com"

RUN apk update && \
    apk upgrade && \
    rm -rf /var/cache/apk/*

COPY --from=0 /go/src/github.com/mosuka/cete/bin/* /usr/bin/
COPY --from=0 /go/src/github.com/mosuka/cete/docker-entrypoint.sh /usr/bin/

EXPOSE 5050 6060 8080

ENTRYPOINT [ "/usr/bin/docker-entrypoint.sh" ]
CMD        [ "cete", "--help" ]
