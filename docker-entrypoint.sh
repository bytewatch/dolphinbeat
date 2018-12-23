#!/bin/sh

PKG_DIR=/dolphinbeat

export DATA_DIR=/data
export HTTP_SERVER_ADDR=":8080"

if [ -z "$MYSQL_ADDR" ]; then
    echo >&2 'env MYSQL_ADDR can not be empty'
    exit 1
fi

if [ -z "$MYSQL_USER" ]; then
    export MYSQL_USER="root"
fi

if [ -z "$MYSQL_PASSWORD" ]; then
    export MYSQL_PASSWORD=""
fi

if [ -z "$MYSQL_CHARSET" ]; then
    export MYSQL_CHARSET="utf8"
fi

if [ -z "$FLAVOR" ]; then
    export FLAVOR="mysql"
fi

if [ -z "$GTID_ENABLED" ]; then
    export GTID_ENABLED="true"
fi

if [ -z "$SKIP_MASTER_DATA" ]; then
    export SKIP_MASTER_DATA="true"
fi

envsubst <${PKG_DIR}/etc/dolphinbeat.toml.template >${DATA_DIR}/dolphinbeat.toml
if [ $? -ne 0 ]; then
    echo >&2 'envsubst error'
    exit 1
fi
mkdir -p ${DATA_DIR}/etc
mkdir -p ${DATA_DIR}/logs

if [ $# -eq 0 ]; then
    cd ${PKG_DIR}/bin
    exec dumb-init ./dolphinbeat --cfg=${DATA_DIR}/dolphinbeat.toml
else
   exec $@
fi

