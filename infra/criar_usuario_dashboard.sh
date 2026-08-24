#!/usr/bin/env bash
# =============================================================================
# criar_usuario_dashboard.sh
# Cria um usuário IAM SEPARADO, só com permissão de leitura no Athena/S3/Glue
# e de invocar o Bedrock — pra usar nas credenciais do dashboard público
# (Streamlit Community Cloud), sem nunca expor o usuário de admin (vita-dev).
#
# Uso: cd infra && chmod +x criar_usuario_dashboard.sh && ./criar_usuario_dashboard.sh
# =============================================================================
set -euo pipefail

USER_NAME="vita-dashboard-readonly"

echo "-- Criando usuário IAM ${USER_NAME} --"
if aws iam get-user --user-name "${USER_NAME}" >/dev/null 2>&1; then
  echo "Usuário já existe, pulando criação."
else
  aws iam create-user --user-name "${USER_NAME}"
fi

echo "-- Anexando policy de somente leitura --"
aws iam put-user-policy \
  --user-name "${USER_NAME}" \
  --policy-name VITA-Dashboard-ReadOnly \
  --policy-document file://iam_dashboard_readonly_policy.json

echo "-- Gerando Access Key --"
CHAVE_JSON=$(aws iam create-access-key --user-name "${USER_NAME}")

ACCESS_KEY=$(echo "${CHAVE_JSON}" | grep -o '"AccessKeyId": "[^"]*' | cut -d'"' -f4)
SECRET_KEY=$(echo "${CHAVE_JSON}" | grep -o '"SecretAccessKey": "[^"]*' | cut -d'"' -f4)

cat <<EOF

=============================================================
Usuário de dashboard (somente leitura) criado com sucesso!

  Nome do usuário: ${USER_NAME}
  Access Key ID:   ${ACCESS_KEY}
  Secret Access Key: ${SECRET_KEY}

⚠️  GUARDE essas credenciais em local seguro AGORA — o Secret Access
    Key não pode ser recuperado depois, só gerar um novo.

Use esses valores no "Secrets" do Streamlit Community Cloud (não
no seu .aws/credentials pessoal, e NUNCA direto no código).
=============================================================
EOF
