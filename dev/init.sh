#!/bin/sh
# One-shot bootstrap for the Tier 2 dev stack (run by the `init` service in
# dev/compose.yaml). Idempotent: safe to re-run on `make dev-up`.
#   1. create the `warehouse` bucket in SeaweedFS
#   2. fetch a Polaris OAuth2 token (client-credentials)
#   3. create the `warzone` Polaris catalog backed by s3://warehouse
set -eu

S3_ENDPOINT="http://seaweedfs:8333"
POLARIS="http://polaris:8181"
BUCKET="warehouse"
CATALOG="warzone"

echo "[init] creating bucket s3://$BUCKET in SeaweedFS"
# `mb` on an existing bucket returns non-zero; tolerate it (idempotent).
aws --endpoint-url "$S3_ENDPOINT" s3 mb "s3://$BUCKET" 2>/dev/null \
  || echo "[init] bucket already exists (ok)"

echo "[init] fetching Polaris OAuth2 token"
TOKEN=$(curl -sf -X POST "$POLARIS/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials" \
  -d "client_id=root" \
  -d "client_secret=s3cr3t" \
  -d "scope=PRINCIPAL_ROLE:ALL" | jq -r '.access_token')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "[init] ERROR: failed to obtain Polaris token" >&2
  exit 1
fi

echo "[init] creating catalog '$CATALOG' (base s3://$BUCKET on SeaweedFS)"
CODE=$(curl -s -o /tmp/resp.json -w '%{http_code}' \
  -X POST "$POLARIS/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "type": "INTERNAL",
      "name": "'"$CATALOG"'",
      "properties": { "default-base-location": "s3://'"$BUCKET"'" },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://'"$BUCKET"'/*"],
        "roleArn": "arn:aws:iam::000000000000:role/dev",
        "endpoint": "'"$S3_ENDPOINT"'",
        "pathStyleAccess": true
      }
    }
  }')

# 201 created, 409 already exists — both fine on a re-run.
if [ "$CODE" = "201" ] || [ "$CODE" = "409" ]; then
  echo "[init] catalog ready (HTTP $CODE)"
else
  echo "[init] ERROR: catalog create failed (HTTP $CODE):" >&2
  cat /tmp/resp.json >&2 || true
  exit 1
fi

# Grant the root principal data access on the catalog. Without this, the
# querier's LOAD_TABLE_WITH_READ_DELEGATION (DuckDB requesting vended read
# creds) is rejected as unauthorized. Idempotent: 2xx create, 409 exists.
M="$POLARIS/api/management/v1"
mgmt() { # METHOD PATH BODY -> tolerate 200/201/204/409
  code=$(curl -s -o /dev/null -w '%{http_code}' -X "$1" "$M$2" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d "$3")
  case "$code" in 200|201|204|409) : ;; *) echo "[init] ERROR: $1 $2 -> HTTP $code" >&2; exit 1 ;; esac
}

echo "[init] granting catalog data access to principal 'root'"
mgmt POST "/catalogs/$CATALOG/catalog-roles" '{"catalogRole":{"name":"admin"}}'
mgmt PUT "/catalogs/$CATALOG/catalog-roles/admin/grants" \
  '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'
mgmt PUT "/principal-roles/service_admin/catalog-roles/$CATALOG" \
  '{"catalogRole":{"name":"admin"}}'

echo "[init] done"
