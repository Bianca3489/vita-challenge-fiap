#!/usr/bin/env bash
# =============================================================================
# setup_aws_infra.sh
# Provisiona toda a infraestrutura base do projeto VITA na AWS:
#   - buckets S3 (bronze / silver / gold / scripts / resultados athena)
#   - IAM Role usada por Glue, Step Functions e Lambda
#   - Glue Data Catalog databases (vita_bronze, vita_silver, vita_gold)
#
# Requisitos: AWS CLI v2 configurado (aws configure) com permissão de admin
#             ou permissões equivalentes a IAM/S3/Glue.
# =============================================================================
set -euo pipefail

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region || echo "us-east-1")
SUFFIX="${ACCOUNT_ID}"

DATALAKE_BUCKET="vita-datalake-${SUFFIX}"
SCRIPTS_BUCKET="vita-glue-scripts-${SUFFIX}"
ATHENA_RESULTS_BUCKET="vita-athena-results-${SUFFIX}"
ROLE_NAME="VITA-Glue-Role"

echo "== Conta AWS: ${ACCOUNT_ID} | Região: ${REGION} =="

echo "-- Criando buckets S3 --"
for BUCKET in "${DATALAKE_BUCKET}" "${SCRIPTS_BUCKET}" "${ATHENA_RESULTS_BUCKET}"; do
  if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
    echo "Bucket ${BUCKET} já existe, pulando."
  else
    if [ "${REGION}" = "us-east-1" ]; then
      aws s3api create-bucket --bucket "${BUCKET}"
    else
      aws s3api create-bucket --bucket "${BUCKET}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
    fi
    aws s3api put-bucket-versioning --bucket "${BUCKET}" \
      --versioning-configuration Status=Enabled
    echo "Bucket ${BUCKET} criado."
  fi
done

echo "-- Criando prefixos (bronze/silver/gold) no datalake --"
for LAYER in bronze silver gold; do
  aws s3api put-object --bucket "${DATALAKE_BUCKET}" --key "${LAYER}/" >/dev/null
done

echo "-- Criando IAM Role ${ROLE_NAME} --"
if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  echo "Role já existe, atualizando policies."
else
  aws iam create-role \
    --role-name "${ROLE_NAME}" \
    --assume-role-policy-document file://iam_glue_trust_policy.json
fi

aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name VITA-Glue-Permissions \
  --policy-document file://iam_glue_permissions_policy.json

aws iam attach-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

echo "-- Criando Glue Data Catalog databases --"
for DB in vita_bronze vita_silver vita_gold; do
  aws glue get-database --name "${DB}" >/dev/null 2>&1 || \
    aws glue create-database --database-input "{\"Name\":\"${DB}\"}"
done

cat <<EOF

=============================================================
Infraestrutura pronta!

  Datalake bucket:        s3://${DATALAKE_BUCKET}
  Scripts bucket:         s3://${SCRIPTS_BUCKET}
  Athena results bucket:  s3://${ATHENA_RESULTS_BUCKET}
  IAM Role (ARN):         arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}
  Glue databases:         vita_bronze, vita_silver, vita_gold

Exporte estas variáveis antes de rodar o resto do pipeline:

  export VITA_DATALAKE_BUCKET=${DATALAKE_BUCKET}
  export VITA_SCRIPTS_BUCKET=${SCRIPTS_BUCKET}
  export VITA_ATHENA_RESULTS_BUCKET=${ATHENA_RESULTS_BUCKET}
  export VITA_GLUE_ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}
  export AWS_REGION=${REGION}
=============================================================
EOF
