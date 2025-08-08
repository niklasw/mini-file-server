#!/usr/bin/env bash

cd "${0%/*}"

LOG_DIR="logs"
PORT=${1:-4445}

mkdir -p $LOG_DIR

function already_running()
{
    if ss -tulpn | grep -q ":$PORT\>"
    then
        echo "Server seems already be running on port $PORT"
        exit 1
    fi
}

if [[ "$1" == stop ]]
then
    SPID=$(<"$LOG_DIR/PID")
    if kill $SPID
    then
        echo killed $SPID
    fi
    exit 0
fi

already_running
[[ -d venv ]] && source venv/bin/activate
./file-server.py $PORT > $LOG_DIR/server.log 2>&1 &

