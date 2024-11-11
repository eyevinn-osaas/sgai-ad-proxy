#!/bin/sh

if [ -z "$VAST_ENDPOINT" ]; then
  echo "VAST_ENDPOINT is required"
  exit 1
fi

if [ -z "$ORIGIN_URL" ]; then
  echo "ORIGIN_URL is required"
  exit 1
fi

COUCHDB_OPTS=""

if [! -z "$COUCHDB_ENDPOINT" ]; then
  if [ -z "$COUCHDB_USER" ]; then
    echo "COUCHDB_USER is required"
    exit 1
  fi
  if [ -z "$COUCHDB_PASSWORD" ]; then
    echo "COUCHDB_PASSWORD is required"
    exit 1
  fi
  if [ -z "$COUCHDB_TABLE" ]; then
    echo "COUCHDB_TABLE is required"
    exit 1
  fi
  COUCHDB_OPTS="--couchdb-endpoint $COUCHDB_ENDPOINT --couchdb-table $COUCHDB_TABLE" 
fi

/app/ad_proxy 0.0.0.0 ${PORT:-8080} \
  ${ORIGIN_URL} \
  ${VAST_ENDPOINT} \
  --ad-insertion-mode ${INSERTION_MODE:-static} \
  --interstitials-address https://${OSC_HOSTNAME} \
  ${COUCHDB_OPTS}
