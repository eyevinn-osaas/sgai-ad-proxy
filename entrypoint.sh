#!/bin/sh

if [ -z "$VAST_ENDPOINT" ]; then
  echo "VAST_ENDPOINT is required"
  exit 1
fi

if [ -z "$ORIGIN_HOST" ] && [ -z "$ORIGIN_URL" ]; then
  echo "Either ORIGIN_HOST or ORIGIN_URL is required"
  exit 1
fi

# Build origin argument: prefer ORIGIN_URL (full playlist URL) over ORIGIN_HOST (base only)
if [ -n "$ORIGIN_URL" ]; then
  ORIGIN_ARG="${ORIGIN_URL}"
else
  ORIGIN_ARG="--origin-host ${ORIGIN_HOST}"
fi

# Allow full override of interstitials address (e.g. http:// for local dev)
if [ -z "$INTERSTITIALS_ADDRESS" ]; then
  INTERSTITIALS_ADDRESS="https://${OSC_HOSTNAME}"
fi

/app/ad_proxy 0.0.0.0 ${PORT:-8080} \
  ${VAST_ENDPOINT} \
  ${ORIGIN_ARG} \
  --ad-insertion-mode ${INSERTION_MODE:-static} \
  --interstitials-address ${INTERSTITIALS_ADDRESS}
