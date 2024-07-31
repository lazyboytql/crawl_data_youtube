#!/bin/sh
set -e

if [ "$#" -eq 0 ]; then
    exec python crawl.py
else
    exec "$@"
fi
