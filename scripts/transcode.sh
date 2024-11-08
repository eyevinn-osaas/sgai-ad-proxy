#!/bin/sh

ENCORE_BASE_URL=https://eyevinn-sgai.encore.prod.osaas.io
ENCORE_CALLBACK_BASE_URL=https://eyevinn-sgai.eyevinn-encore-callback-listener.auto.prod.osaas.io

CREATIVE_URL=$1

if [ -z "$CREATIVE_URL" ]; then
  echo "Usage: $0 <creative-url>"
  exit 1
fi

if [ -z "$OSC_ACCESS_TOKEN" ]; then
  echo "OSC_ACCESS_TOKEN is not set"
  exit 1
fi

SAT=`curl -X POST -H 'Content-Type: application/json' \
     -H "x-pat-jwt: Bearer $OSC_ACCESS_TOKEN" \
     -d '{ "serviceId": "encore" }' https://token.svc.prod.osaas.io/servicetoken \
      | jq -r '.token'`

FILENAME=`basename $CREATIVE_URL`
BASENAME=`echo "${FILENAME%.*}"`

JOB=`cat <<EOF
{
  "externalId": "$BASENAME",
  "profile": "program",
  "outputFolder": "/usercontent/",
  "baseName": "$BASENAME",
  "progressCallbackUri": "$ENCORE_CALLBACK_BASE_URL/encoreCallback",
  "inputs": [
    {
      "uri": "$CREATIVE_URL",
      "copyTs": true,
      "type": "AudioVideo"
    }
  ]  
}
EOF
`

if `echo $JOB | jq empty`; then
  echo "Submitting job to Encore"
  RESPONSE=`curl -X POST -H 'Content-Type: application/json' \
      -H "Authorization: Bearer $SAT" \
      -d "$JOB" $ENCORE_BASE_URL/encoreJobs`
  echo "$RESPONSE" | jq -r '.id'
else
  echo "Job is not valid JSON"
fi  

