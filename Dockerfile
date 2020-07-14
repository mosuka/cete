FROM alpine:3.11

LABEL maintainer="Minoru Osuka minoru.osuka@gmail.com"
LABEL maintainer="Vinícius Niche Correa viniciusnichecorrea@gmail.com"

RUN apk update && \
    rm -rf /var/cache/apk/*

RUN	addgroup cete \
    && adduser -S cete -u 1000 -G cete

USER cete

COPY --chown=cete:cete bin/cete /usr/bin/

EXPOSE 7000 8000 9000

ENTRYPOINT [ "/usr/bin/cete" ]
CMD        [ "start" ]
