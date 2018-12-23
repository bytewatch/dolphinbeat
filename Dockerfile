# Builder image
FROM golang:alpine as builder

RUN apk add --no-cache \
    wget \
    make \
    git \
    gettext

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
 && chmod +x /usr/local/bin/dumb-init

COPY . /go/src/github.com/bytewatch/dolphinbeat

WORKDIR /go/src/github.com/bytewatch/dolphinbeat

RUN make

# Executable image
FROM alpine

RUN apk add --update libintl && \
    apk add --no-cache \
    mariadb-client && \
    rm -rf /usr/bin/mysql_find_rows \ 
    /usr/bin/mysql_waitpid /usr/bin/mysqlshow \ 
    /usr/bin/mysql_fix_extensions  /usr/bin/mysqldumpslow \
    /usr/bin/mysqladmin /usr/bin/mysqlcheck \ 
    /usr/bin/myisam_ftdump  /usr/bin/mysqlimport  \ 
    /usr/bin/mysqlaccess

COPY --from=builder /go/src/github.com/bytewatch/dolphinbeat/build/dolphinbeat /dolphinbeat
COPY --from=builder /usr/local/bin/dumb-init /usr/local/bin/
COPY --from=builder /usr/bin/envsubst /usr/local/bin/

COPY docker-entrypoint.sh /usr/local/bin/


VOLUME /data

EXPOSE 8080

ENTRYPOINT ["docker-entrypoint.sh"]
