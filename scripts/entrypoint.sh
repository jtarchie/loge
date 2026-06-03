#!/bin/sh
set -e

# loge reads its S3 connection from the env vars that `fly storage create`
# injects, via kong env tags:
#   --s3-bucket   <- BUCKET_NAME
#   --s3-endpoint <- AWS_ENDPOINT_URL_S3
#   --s3-region   <- AWS_REGION
# and AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are read by the AWS SDK directly.
# So we only pass the loge-specific settings here.
#
# Reads of cold segments are public, path-based HTTP GETs, so the bucket must be
# public (or objects uploaded public-read) and the read URL must point at the
# Tigris public domain. Override any of the ${VAR:-default} values via fly secrets
# or [env] in fly.toml.

exec loge \
  --port "${PORT:-8080}" \
  --output-path "${OUTPUT_PATH:-/data}" \
  --buckets "${BUCKETS:-4}" \
  --payload-size "${PAYLOAD_SIZE:-100000}" \
  --compact-interval "${COMPACT_INTERVAL:-15s}" \
  --query-concurrency "${QUERY_CONCURRENCY:-16}" \
  --s3-force-path-style \
  --s3-read-url-base "${S3_READ_URL_BASE:-https://${BUCKET_NAME}.t3.tigrisfiles.io}" \
  --s3-acl "${S3_ACL:-public-read}" \
  --s3-rotate-age "${S3_ROTATE_AGE:-2m}" \
  --s3-rotate-interval "${S3_ROTATE_INTERVAL:-30s}" \
  --s3-rotate-grace "${S3_ROTATE_GRACE:-1m}"
