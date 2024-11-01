#!/bin/sh

if [ -z "$VAST_ENDPOINT" ]; then
  echo "VAST_ENDPOINT is required"
  exit 1
fi

if [ -z "$ORIGIN_HOST" ]; then
  echo "ORIGIN_HOST is required"
  exit 1
fi

if [ -z "$PLAYLIST_PATH" ]; then
  echo "PLAYLIST_PATH is required"
  exit 1
fi

/app/ad_proxy 0.0.0.0 ${PORT:-8080} \
  ${ORIGIN_HOST} ${ORIGIN_PORT:-443} \
  ${PLAYLIST_PATH} \
  ${VAST_ENDPOINT} -a advanced -i ${INSERTION_MODE:-static}