FROM golang:1.14.0-alpine3.11

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

FROM alpine:3.11

MAINTAINER Minoru Osuka "minoru.osuka@gmail.com"

RUN apk update && \
    apk upgrade && \
    rm -rf /var/cache/apk/*

COPY --from=0 /go/src/github.com/mosuka/cete/bin/* /usr/bin/
COPY --from=0 /go/src/github.com/mosuka/cete/docker-entrypoint.sh /usr/bin/

EXPOSE 7000 8000 9000

ENTRYPOINT [ "/usr/bin/docker-entrypoint.sh" ]
CMD        [ "cete", "start" ]
