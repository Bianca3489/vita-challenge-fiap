# GUIA DO ZERO — AWS para quem nunca usou

Este guia assume **zero conhecimento prévio de AWS**. Vamos do "criar a conta" até
"ver o dashboard funcionando". Siga na ordem — cada parte diz claramente se é
"pelo site (console)" ou "pelo terminal (CLI)".

Tempo estimado: 2h a 3h na primeira vez (a maior parte é espera de download/processamento).

---

## PARTE 0 — Conceitos rápidos (2 minutos de leitura, não pule)

- **Console AWS** = o site (aws.amazon.com/console). Você clica e configura visualmente.
- **AWS CLI** = um programa que você instala no seu computador para dar comandos
  (tipo `git`, só que para a AWS). Usamos ele para automatizar tudo com o script
  que já vem pronto no pacote (`infra/setup_aws_infra.sh`).
- **Região** = onde seus dados/serviços ficam fisicamente (ex.: `us-east-1` = Norte da
  Virgínia). Use **sempre a mesma região** em tudo neste projeto. Sugestão: `us-east-1`
  (mais barata e com mais serviços disponíveis).
- **S3** = onde os arquivos ficam guardados (o "datalake").
- **Glue** = onde o código Python/Spark roda para transformar os dados.
- **Athena** = onde você faz consultas SQL nos dados guardados no S3.
- **IAM** = onde ficam usuários, senhas e permissões.

---

## PARTE 1 — Criar a conta AWS (pelo site)

1. Acesse **https://aws.amazon.com** → clique em **"Criar uma conta AWS"**.
2. Informe e-mail, senha e nome da conta (ex.: `vita-challenge-fiap`).
3. Informe um cartão de crédito (é exigido, mas o Free Tier cobre praticamente
   tudo que vamos usar aqui — veja custos estimados na Parte 8).
4. Confirme telefone e escolha o plano de suporte **"Basic support - Free"**.
5. Você vai cair no **Console AWS**. Guarde o link de login e o **Account ID**
   (aparece no canto superior direito, é um número de 12 dígitos).

> ⚠️ Nunca use o usuário **root** (o login principal) no dia a dia. Vamos criar
> um usuário separado só para o projeto (Parte 2).

---

## PARTE 2 — Criar um usuário de trabalho (IAM) — pelo site

1. No Console, na barra de busca do topo, digite **IAM** e entre no serviço.
2. Menu lateral → **Users** → **Create user**.
3. Nome do usuário: `vita-dev`. Clique **Next**.
4. Em "Permissions options", escolha **Attach policies directly**.
5. Marque estas policies (busque cada nome na caixa de busca):
   - `AdministratorAccess` *(mais simples para o projeto acadêmico; se quiser
     mais restrito depois, pode trocar por policies específicas de S3/Glue/Athena/IAM)*
6. **Next** → **Create user**.
7. Clique no usuário `vita-dev` recém-criado → aba **Security credentials**.
8. Em "Access keys" → **Create access key** → escolha **Command Line Interface (CLI)**
   → marque a caixinha de confirmação → **Next** → **Create access key**.
9. **Copie e guarde em local seguro**: `Access Key ID` e `Secret Access Key`
   (a secret key só aparece **uma vez**, se perder tem que gerar outra).

---

## PARTE 3 — Instalar e configurar o AWS CLI (pelo terminal)

### Instalar
- **Windows**: baixe e rode o instalador em
  `https://awscli.amazonaws.com/AWSCLIV2.msi`
- **Mac**: `brew install awscli` (ou baixe o `.pkg` do site da AWS)
- **Linux**:
  ```bash
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
  unzip awscliv2.zip && sudo ./aws/install
  ```

### Verificar instalação
```bash
aws --version
```

### Configurar com suas credenciais
```bash
aws configure
```
Vai pedir 4 coisas:
```
AWS Access Key ID [None]: <cole o Access Key ID da Parte 2>
AWS Secret Access Key [None]: <cole o Secret Access Key da Parte 2>
Default region name [None]: us-east-1
Default output format [None]: json
```

### Testar se funcionou
```bash
aws sts get-caller-identity
```
Se aparecer um JSON com `"UserId"`, `"Account"` e `"Arn"` mencionando `vita-dev`,
está tudo certo. Se der erro de credenciais, repita `aws configure`.

---

## PARTE 4 — Preparar o projeto no seu computador

1. Descompacte o `vita_challenge_aws.zip` em uma pasta, por exemplo `~/vita_challenge`.
2. Instale o Python (3.10+) se ainda não tiver: `https://www.python.org/downloads/`
3. Abra o terminal dentro da pasta do projeto e instale as dependências:
   ```bash
   cd vita_challenge/ingestion
   pip install -r requirements.txt
   ```

### Teste rápido SEM AWS (para você ver que os scripts funcionam sozinhos)
```bash
python ingest_sih_sus.py --uf SP --ano 2025 --mes 6
python ingest_cnes_api.py --uf SP
python ingest_csv_auxiliar.py
```
Como ainda não configuramos o bucket S3 de verdade, os scripts vão avisar
"Falha ao subir no S3... salvo localmente" e gravar tudo em uma pastinha
`output_local/`. Isso é esperado e mostra que a lógica de ingestão (com o
gerador de dados sintéticos de segurança) está funcionando.

Também dá para testar o cálculo do índice de pressão hospitalar sem AWS:
```bash
cd ../etl
pip install pandas numpy scikit-learn
python ml_indice_pressao_hospitalar.py --output indice_teste.csv
```
Isso já imprime no terminal um ranking de hospitais por risco — é o mesmo
cálculo que depois vai rodar dentro do Glue.

---

## PARTE 5 — Criar a infraestrutura na AWS (pelo terminal)

```bash
cd vita_challenge/infra
chmod +x setup_aws_infra.sh
./setup_aws_infra.sh
```

Esse script (que já vem pronto) faz automaticamente:
- cria 3 buckets S3 (`vita-datalake-<sua-conta>`, `vita-glue-scripts-<sua-conta>`,
  `vita-athena-results-<sua-conta>`)
- cria os "prefixos" bronze/silver/gold dentro do datalake
- cria a IAM Role `VITA-Glue-Role` (usada pelo Glue e Step Functions para ter
  permissão de ler/escrever no S3, rodar consultas no Athena, etc.)
- cria os 3 databases no Glue Data Catalog: `vita_bronze`, `vita_silver`, `vita_gold`

No final ele imprime algo assim — **copie e cole esses `export` no seu terminal**,
vamos usar essas variáveis nos próximos passos:
```
export VITA_DATALAKE_BUCKET=vita-datalake-123456789012
export VITA_SCRIPTS_BUCKET=vita-glue-scripts-123456789012
export VITA_ATHENA_RESULTS_BUCKET=vita-athena-results-123456789012
export VITA_GLUE_ROLE_ARN=arn:aws:iam::123456789012:role/VITA-Glue-Role
export AWS_REGION=us-east-1
```

### Conferir pelo site (opcional, mas recomendado na primeira vez)
- Console → busque **S3** → você deve ver os 3 buckets criados.
- Console → busque **Glue** → menu lateral **Data Catalog → Databases** →
  você deve ver `vita_bronze`, `vita_silver`, `vita_gold`.
- Console → busque **IAM** → **Roles** → deve existir `VITA-Glue-Role`.

---

## PARTE 6 — Rodar a ingestão de verdade (gravando no S3)

Agora que o bucket existe, exporte a variável de ambiente que os scripts usam
e rode de novo:
```bash
cd ../ingestion
export DATALAKE_BUCKET=$VITA_DATALAKE_BUCKET
python ingest_sih_sus.py --uf SP --ano 2025 --mes 6
python ingest_cnes_api.py --uf SP
python ingest_csv_auxiliar.py
```
Dessa vez a mensagem deve ser `"Gravado no Bronze: s3://..."`, sem o aviso de
fallback local.

### Conferir pelo site
Console → **S3** → clique no bucket `vita-datalake-...` → você deve ver as
pastas `bronze/sih_sus/`, `bronze/cnes/`, `bronze/csv_auxiliar/` com arquivos dentro.

---

## PARTE 7 — Subir os scripts de ETL e criar os Glue Jobs

### 7.1 Subir os scripts para o S3 (terminal)
```bash
cd ../etl
aws s3 cp . s3://$VITA_SCRIPTS_BUCKET/etl/ --recursive --exclude "*" \
  --include "glue_bronze_to_silver_sih.py" \
  --include "glue_bronze_to_silver_cnes.py" \
  --include "glue_silver_to_gold_kpis.py"
```

### 7.2 Criar os 3 Glue Jobs (terminal, usando os JSONs já prontos em infra/)
Antes, edite os 3 arquivos `infra/glue_job_*.json` substituindo `<CONTA>` pelo
seu Account ID (o número que você guardou na Parte 1) — ou rode este comando
que faz a substituição automaticamente:
```bash
cd ../infra
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
for f in glue_job_bronze_silver_sih.json glue_job_bronze_silver_cnes.json glue_job_silver_gold.json; do
  sed "s/<CONTA>/${ACCOUNT_ID}/g" "$f" > "/tmp/${f}"
done

aws glue create-job --cli-input-json file:///tmp/glue_job_bronze_silver_sih.json
aws glue create-job --cli-input-json file:///tmp/glue_job_bronze_silver_cnes.json
aws glue create-job --cli-input-json file:///tmp/glue_job_silver_gold.json
```

### 7.3 Rodar os jobs manualmente (a primeira vez, pelo site é mais visual)
Console → busque **Glue** → menu lateral **ETL Jobs** → você verá os 3 jobs
criados (`vita-bronze-to-silver-sih`, `vita-bronze-to-silver-cnes`,
`vita-silver-to-gold`).

**Ordem importa**: rode primeiro os dois `bronze-to-silver`, espere os dois
terminarem (status **Succeeded**), depois rode o `vita-silver-to-gold`.

Para cada job: clique nele → botão **Run** (canto superior direito) → acompanhe
na aba **Runs** até o Status virar **Succeeded** (leva de 2 a 6 minutos por job,
porque o Glue precisa "esquentar" um cluster Spark do zero a cada execução).

> Se der erro, clique no run com falha → **Logs** (Amazon CloudWatch) para ver
> a mensagem de erro do Spark/Python.

Alternativa pelo terminal (se preferir não clicar):
```bash
aws glue start-job-run --job-name vita-bronze-to-silver-sih
aws glue start-job-run --job-name vita-bronze-to-silver-cnes
# espere terminarem, depois:
aws glue start-job-run --job-name vita-silver-to-gold
```

### Conferir pelo site
Console → **S3** → bucket do datalake → deve aparecer `silver/` e depois `gold/`
com as pastas `fato_internacoes/`, `dim_hospital/`, `kpi_pressao_hospitalar/`, etc.

---

## PARTE 8 — Consultar os dados no Athena (pelo site é o mais fácil)

1. Console → busque **Athena** → **Query editor**.
2. Na primeira vez, o Athena pede um "Query result location" — clique em
   **Edit settings** e informe `s3://vita-athena-results-<sua-conta>/`.
3. Cole o conteúdo do arquivo `analytics/athena_ddl.sql` (troque
   `<DATALAKE_BUCKET>` pelo nome real do seu bucket) e clique **Run**.
4. Depois cole o conteúdo de `analytics/athena_gold_views.sql` e rode também.
5. Teste uma consulta simples para confirmar que tem dado:
   ```sql
   SELECT * FROM vita_gold.vw_ranking_hospitais_criticos LIMIT 10;
   ```
   Se aparecer uma tabela de resultado, está tudo funcionando ponta a ponta! 🎉

---

## PARTE 9 — Conectar o Power BI

Siga o arquivo `dashboards/powerbi_athena_connection.md` (instalar o driver
ODBC do Athena, criar a conexão, montar as páginas). Use as mesmas credenciais
(`Access Key ID` / `Secret Access Key`) do usuário `vita-dev`.

---

## PARTE 10 — Orquestração automática (opcional, faça por último)

Para o MVP e a apresentação, **rodar manualmente os jobs do Glue (Parte 7.3) já
é suficiente** — o requisito da Sprint 2 é ter o MVP funcionando, não
necessariamente 100% automatizado.

Se quiser ir além (bom para o critério "Inovação"), crie a orquestração com
Step Functions:
```bash
cd ../orchestration
aws stepfunctions create-state-machine \
  --name VITA-Pipeline \
  --definition file://step_functions_state_machine.json \
  --role-arn $VITA_GLUE_ROLE_ARN
```
Isso requer também publicar as 3 funções Lambda de ingestão (arquivo
`lambda_trigger_glue.py`) — se quiser ajuda para empacotar e publicar essas
Lambdas, me avise que eu te dou o passo a passo específico disso também.

---

## PARTE 11 — Custos e como não ser surpreendido pela fatura

- Configure um **orçamento de alerta**: Console → busque **Billing** →
  **Budgets** → **Create budget** → tipo "Cost budget" → defina, por exemplo,
  US$ 5,00/mês → configure alerta por e-mail em 80% do valor.
- Depois de usar, **pare/apague o que não precisa mais**:
  - Glue Jobs não cobram parados, só quando rodam — não precisa deletar.
  - S3: cobra por armazenamento, mas é centavos para o volume deste projeto.
  - Se criar o Step Functions/Lambda, também não tem custo fixo parado.
  - **Evite o Amazon MWAA** (Airflow gerenciado) — esse sim tem custo fixo por
    hora mesmo parado. Use o `docker-compose-airflow.yml` (local, grátis) se
    quiser mostrar Airflow no vídeo.

---

## Resumo do fluxo (para colar num quadro/checklist)

- [ ] Parte 1: conta AWS criada
- [ ] Parte 2: usuário `vita-dev` + access keys criados
- [ ] Parte 3: AWS CLI instalado e `aws configure` funcionando
- [ ] Parte 4: projeto descompactado, dependências instaladas, teste local ok
- [ ] Parte 5: `setup_aws_infra.sh` rodado, buckets/roles/databases criados
- [ ] Parte 6: ingestão gravando de verdade no S3 (bronze)
- [ ] Parte 7: scripts no S3, Glue Jobs criados e rodados (silver + gold)
- [ ] Parte 8: tabelas/views criadas no Athena, consulta de teste retornando dado
- [ ] Parte 9: Power BI conectado, páginas montadas
- [ ] Parte 10 (opcional): Step Functions orquestrando tudo
- [ ] Parte 11: orçamento de alerta configurado
