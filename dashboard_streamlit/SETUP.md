# Dashboard VITA (Streamlit) — Guia de Setup

Alternativa ao Power BI: um dashboard em Python, rodando direto sobre o
Amazon Athena, com o "Ask VITA" (chat com IA) integrado na mesma tela.
Mais rápido de montar, mais fácil de mostrar num vídeo pitch, e continua
sendo "o link da aplicação funcionando" pedido no edital.

---

## 1. Instalar

```bash
cd ~/vita_challenge/dashboard_streamlit    # ajuste pro caminho real do seu projeto
pip3 install -r requirements.txt
```

## 2. Pré-requisitos (você já tem tudo isso configurado do resto do projeto)

- Credenciais AWS configuradas (`aws configure` — já feito)
- As views do Athena criadas (`analytics/athena_gold_views.sql` — já feito)
- Acesso ao **Amazon Bedrock** habilitado na sua conta, com o modelo Claude
  liberado (só necessário pra aba "Ask VITA" — as outras 4 abas funcionam
  sem isso). Se nunca usou Bedrock nessa conta:
  - Console → busque **Bedrock** → menu lateral **Model access** →
    **Manage model access** → marque **Claude 3.5 Sonnet** (Anthropic) →
    **Save changes** (a liberação é quase instantânea)

## 3. Rodar localmente

```bash
export AWS_REGION=us-east-1
export ATHENA_DATABASE=vita_gold
export ATHENA_WORKGROUP=primary
streamlit run app.py
```

Abre automaticamente no navegador em `http://localhost:8501`. As 5 abas:
**Visão Geral** (KPIs + gráficos), **Visão Evolutiva** (linha do tempo por
doença), **Mapa de Pressão** (mapa interativo), **Alertas** (ranking de
hospitais críticos) e **Ask VITA** (chat).

## 4. Para a entrega/vídeo pitch

Duas opções, dependendo do que o edital espera:

### Opção A — Rodar local e gravar a tela (mais simples, recomendado)
Rode `streamlit run app.py`, navegue pelas abas, grave a tela — é
exatamente isso que o vídeo pitch de 5 minutos precisa mostrar
("demonstração hands on" / "MVP em funcionamento").

### Opção B — Publicar com link público (Streamlit Community Cloud, grátis)
Se quiser um **link real** pra colocar no PPTX/portal:

1. Suba o código do dashboard num repositório GitHub **público**
2. Acesse **share.streamlit.io** → **New app** → conecte seu GitHub →
   aponte pro repositório e pro arquivo `app.py`
3. Em **Advanced settings → Secrets**, cole as variáveis de ambiente e
   credenciais (nunca deixe credenciais no código-fonte do repositório!):
   ```toml
   AWS_ACCESS_KEY_ID = "..."
   AWS_SECRET_ACCESS_KEY = "..."
   AWS_REGION = "us-east-1"
   ATHENA_DATABASE = "vita_gold"
   ATHENA_WORKGROUP = "primary"
   ```
4. Deploy — em 1-2 minutos você tem um link público tipo
   `https://vita-fiap.streamlit.app`

⚠️ **Segurança**: se for publicar com link público, crie um usuário IAM
**separado e restrito** só pra isso (permissão de leitura no Athena/S3,
nada de admin) — nunca reutilize a chave `vita-dev` de admin que você usa
no dia a dia. Assim que a apresentação terminar, é uma boa prática
desativar ou apagar essa chave.

## 5. Se algo der erro

- **"Access Denied" no Athena**: confirme que as credenciais em uso têm
  permissão de `athena:*`, `glue:Get*` e leitura nos buckets S3 do datalake
  e de resultados do Athena.
- **Aba "Ask VITA" dá erro de acesso ao Bedrock**: confirme que liberou o
  modelo em **Model access** (passo 2) e que está na região certa (nem
  todo modelo está disponível em toda região — `us-east-1` costuma ter
  cobertura ampla).
- **Mapa não mostra nada**: confira se `vw_pressao_por_regiao` tem
  `latitude`/`longitude` preenchidos (`SELECT * FROM vita_gold.vw_pressao_por_regiao WHERE latitude IS NULL` —
  se vier muita coisa null, os hospitais correspondentes não tinham
  coordenada no CNES).
