"""
ingest_sih_sus.py
==================
FONTE 1 (tabela relacional) do desafio Oracle/FIAP: SIH/SUS — Sistema de
Informações Hospitalares. Contém internações, valor pago, permanência média,
município e período. É AQUI que fica o diagnóstico (campo DIAG_PRINC / CID-10).

Comportamento padrão (Bronze = fiel à fonte, sem filtro de doença):
  Ingere TODOS os meses de 2020-01 até 2026-07 (agosto/2026 ainda não fechou,
  por isso paramos em julho), para a UF informada, SEM filtrar por CID.
  A escolha de quais doenças/períodos usar fica para as camadas Silver/Gold.

Estratégia de download:
  1) Tenta baixar dados reais via biblioteca `PySUS` (lê direto do FTP público
     do DATASUS e converte o .DBC para DataFrame).
  2) Se a rede/FTP do DATASUS estiver indisponível para aquele mês específico,
     cai automaticamente para um gerador sintético com o MESMO schema —
     um mês com problema não derruba os outros 78.

Saída: grava parquet particionado em
  s3://<DATALAKE_BUCKET>/bronze/sih_sus/uf=<UF>/ano_mes=<AAAAMM>/*.parquet

Uso padrão (ingere 2020-01 até 2026-07, SP, sem filtro):
  python ingest_sih_sus.py

Uso com outro intervalo:
  python ingest_sih_sus.py --uf SP --ano_inicio 2022 --mes_inicio 1 \\
      --ano_fim 2023 --mes_fim 12

Uso com filtro de doença aplicado já na ingestão (opcional — normalmente
prefira NÃO filtrar aqui e filtrar depois no Silver/Gold):
  python ingest_sih_sus.py --cid_prefixo A90,A91

Uso "um mês só" (compatibilidade com versões antigas do script):
  python ingest_sih_sus.py --ano 2025 --mes 6

Códigos CID-10 de dengue, se um dia precisar filtrar: A90 (Dengue clássica)
e A91 (Dengue hemorrágica).

Uso como Glue Python Shell Job: os mesmos argumentos via --UF --ANO --MES
"""
import argparse
import io
import logging
import os
import sys
from datetime import date, datetime

import boto3
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest_sih_sus")

DATALAKE_BUCKET = os.environ.get("DATALAKE_BUCKET", "vita-datalake-<CONTA>")

# Colunas alinhadas ao dicionário de dados do SIH/SUS (subconjunto relevante)
SIH_COLUMNS = [
    "N_AIH", "UF_ZI", "MUNIC_RES", "MUNIC_MOV", "DT_INTER", "DT_SAIDA",
    "DIAG_PRINC", "PROC_REA", "VAL_TOT", "DIAS_PERM", "IDADE", "SEXO",
    "MORTE", "CNES",
]


def try_download_pysus(uf: str, ano: int, mes: int) -> pd.DataFrame | None:
    """Tenta baixar dados reais do SIH/SUS via PySUS (API assíncrona, v2.9+).

    A partir da v2.x, o PySUS não tem mais a função simples
    `pysus.online_data.SIH.download(uf, ano, mes)`. Agora é preciso:
      1) instanciar o orquestrador `PySUS()`
      2) pegar o cliente FTP (`get_ftp()`)
      3) listar os datasets e achar o "SIH"
      4) buscar (`search`) o arquivo do grupo RD (AIH Reduzida = internações),
         UF, ano e mês pedidos
      5) baixar e converter para parquet/DataFrame (`download_to_parquet`)

    Como o resto do nosso script é síncrono, rodamos essa parte assíncrona
    com `asyncio.run(...)` por dentro desta função — quem chama nem precisa
    saber que por baixo é assíncrono.
    """
    import asyncio

    async def _baixar() -> pd.DataFrame:
        from pysus.api.client import PySUS
        from pysus.api.ftp.client import FTP

        client = PySUS()
        try:
            # timeout curto (30s) para não travar o lote de 79 meses caso a
            # conexão com o DATASUS esteja fora do ar naquele momento
            client._ftp = FTP(timeout=30)
            await client._ftp.connect()
            ftp = client._ftp
            try:
                datasets = await ftp.datasets()
                sih = next(d for d in datasets if type(d).__name__ == "SIH")

                # IMPORTANTE: NÃO passar group="RD" direto no search() —
                # o campo `.group` de cada arquivo é um objeto (não um texto),
                # então a comparação "objeto != 'RD'" nunca bate e o search
                # sempre retorna vazio. Buscamos só por estado/ano/mês e
                # filtramos o grupo manualmente depois.
                # RD = AIH Reduzida (internações aprovadas) — é a base certa
                # para o que o desafio pede (internações, valor pago, permanência)
                candidatos = await sih.search(state=uf, year=ano, month=mes)
                arquivos = [
                    f for f in candidatos
                    if f.group is not None and f.group.name == "RD"
                ]
                if not arquivos:
                    raise FileNotFoundError(
                        f"Nenhum arquivo SIH/RD encontrado para {uf} {ano}/{mes:02d} "
                        f"({len(candidatos)} candidatos de outros grupos foram encontrados)"
                    )

                parquet_file = await client.download_to_parquet(arquivos[0], timeout=180)
                return await parquet_file.load()
            finally:
                await ftp.close()
        finally:
            client.engine.dispose()

    try:
        log.info(f"Baixando SIH/SUS real via PySUS: UF={uf} {ano}/{mes:02d}")
        df = asyncio.run(_baixar())
        df = df.rename(columns=str.upper)
        cols_presentes = [c for c in SIH_COLUMNS if c in df.columns]
        if not cols_presentes:
            log.warning(
                "PySUS retornou dados, mas nenhuma coluna esperada foi encontrada "
                f"(colunas reais: {list(df.columns)[:15]}...). Usando fallback sintético."
            )
            return None
        return df[cols_presentes]
    except Exception as e:  # noqa: BLE001 - queremos qualquer erro de rede/lib aqui
        log.warning(f"PySUS indisponível ou falhou ({e}). Usando fallback sintético.")
        return None


def generate_synthetic_sih(uf: str, ano: int, mes: int, n_rows: int = 5000) -> pd.DataFrame:
    """Gera uma amostra sintética plausível de internações, mesmo schema do SIH."""
    rng = np.random.default_rng(seed=hash((uf, ano, mes)) % (2**32))
    cids_frequentes = {
        "A90": "Dengue",
        "J189": "Pneumonia",
        "J210": "Bronquiolite/Sd. Respiratória",
        "K358": "Apendicite",
        "G439": "Enxaqueca",
        "S065": "Fratura",
        "A099": "Gastroenterite",
    }
    n_municipios = 12
    municipios = rng.integers(100000, 100000 + n_municipios, size=n_rows)
    cnes_ids = rng.integers(2000000, 2000000 + 40, size=n_rows)
    cids = rng.choice(list(cids_frequentes.keys()), size=n_rows,
                       p=[0.22, 0.20, 0.15, 0.12, 0.10, 0.11, 0.10])
    dias_base = datetime(ano, mes, 1)

    df = pd.DataFrame({
        "N_AIH": [f"{ano}{mes:02d}{i:07d}" for i in range(n_rows)],
        "UF_ZI": uf,
        "MUNIC_RES": municipios,
        "MUNIC_MOV": municipios,
        "DT_INTER": pd.to_datetime(dias_base) + pd.to_timedelta(
            rng.integers(0, 28, size=n_rows), unit="D"),
        "DIAG_PRINC": cids,
        "VAL_TOT": np.round(rng.gamma(shape=2.0, scale=800, size=n_rows), 2),
        "DIAS_PERM": rng.poisson(lam=4, size=n_rows) + 1,
        "IDADE": rng.integers(0, 95, size=n_rows),
        "SEXO": rng.choice(["M", "F"], size=n_rows),
        "MORTE": rng.choice([0, 1], size=n_rows, p=[0.97, 0.03]),
        "CNES": cnes_ids,
    })
    df["DT_SAIDA"] = df["DT_INTER"] + pd.to_timedelta(df["DIAS_PERM"], unit="D")
    log.info(f"Gerados {n_rows} registros sintéticos de SIH/SUS para {uf} {ano}/{mes:02d}")
    return df


CAMPOS_TEXTO = ["N_AIH", "UF_ZI", "DIAG_PRINC", "PROC_REA", "SEXO"]
CAMPOS_DATA = ["DT_INTER", "DT_SAIDA"]
CAMPOS_INTEIROS = ["MUNIC_RES", "MUNIC_MOV", "DIAS_PERM", "IDADE", "MORTE", "CNES"]
CAMPOS_DECIMAIS = ["VAL_TOT"]


def normalizar_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Força um tipo de dado ÚNICO e consistente pra cada coluna, seja o
    DataFrame vindo do download real (PySUS) ou do gerador sintético.

    Por que isso é necessário: ao longo dos 79 meses do histórico, a PySUS
    às vezes devolve campos de data já como datetime64 de verdade (não
    texto) — e ao gravar isso em parquet, vira um tipo TIMESTAMP com
    precisão de nanossegundo que o Spark (nesta versão usada pelo Glue) não
    consegue nem ler o schema, travando o job de ETL inteiro com "Illegal
    Parquet type: INT64 (TIMESTAMP(NANOS))". Outros campos (IDs, contadores)
    também podem variar entre int32/int64/string dependendo da execução.
    Sem essa normalização, cada um dos 79 arquivos pode sair com um "molde"
    ligeiramente diferente, e a camada Silver quebra tentando juntar tudo.
    """
    df = df.copy()

    for col in CAMPOS_TEXTO:
        if col in df.columns:
            df[col] = df[col].astype(str)

    for col in CAMPOS_DATA:
        if col not in df.columns:
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # datetime de verdade -> texto simples 'AAAA-MM-DD' (nunca timestamp)
            df[col] = df[col].dt.strftime("%Y-%m-%d")
        else:
            df[col] = df[col].astype(str)

    for col in CAMPOS_INTEIROS:
        if col in df.columns:
            # Int64 (maiúsculo) = inteiro de 64 bits ANULÁVEL do pandas —
            # sempre a mesma largura, tolera valores ausentes sem virar float
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in CAMPOS_DECIMAIS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    return df


def write_to_bronze(df: pd.DataFrame, uf: str, ano: int, mes: int) -> str:
    df = normalizar_schema(df)
    ano_mes = f"{ano}{mes:02d}"
    key = f"bronze/sih_sus/uf={uf}/ano_mes={ano_mes}/sih_{uf}_{ano_mes}.parquet"
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3 = boto3.client("s3")
    try:
        s3.upload_fileobj(buffer, DATALAKE_BUCKET, key)
        uri = f"s3://{DATALAKE_BUCKET}/{key}"
        log.info(f"Gravado no Bronze: {uri} ({len(df)} linhas)")
        return uri
    except Exception as e:  # noqa: BLE001
        # fallback local, útil para testar sem credenciais AWS configuradas
        local_path = os.path.join("output_local", key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        df.to_parquet(local_path, index=False)
        log.warning(f"Falha ao subir no S3 ({e}). Salvo localmente em {local_path}")
        return local_path



def filtrar_por_cid(df: pd.DataFrame, cid_prefixos: list[str]) -> pd.DataFrame:
    """Filtra o DataFrame mantendo só linhas cujo DIAG_PRINC começa com algum
    dos prefixos informados (ex.: ['A90', 'A91'] para dengue)."""
    if not cid_prefixos:
        return df
    mask = df["DIAG_PRINC"].astype(str).str.upper().str.startswith(
        tuple(p.upper() for p in cid_prefixos)
    )
    filtrado = df[mask].copy()
    log.info(f"Filtro CID {cid_prefixos}: {len(filtrado)} de {len(df)} registros mantidos.")
    return filtrado


def gerar_intervalo_meses(ano_inicio: int, mes_inicio: int,
                           ano_fim: int, mes_fim: int) -> list[tuple[int, int]]:
    """Gera a lista de (ano, mes) do início até o fim, inclusive."""
    inicio = date(ano_inicio, mes_inicio, 1)
    fim = date(ano_fim, mes_fim, 1)
    if inicio > fim:
        raise ValueError("Data de início não pode ser depois da data de fim.")
    meses = []
    atual = inicio
    while atual <= fim:
        meses.append((atual.year, atual.month))
        atual += relativedelta(months=1)
    return meses


def processar_mes(uf: str, ano: int, mes: int, cid_prefixos: list[str] | None) -> int:
    """Baixa/gera um mês, aplica filtro de doença (se houver) e grava no Bronze.
    Retorna a quantidade de linhas gravadas (0 se não sobrou nada após o filtro).
    Nunca propaga exceção — um mês com problema não deve derrubar o lote inteiro."""
    try:
        df = try_download_pysus(uf, ano, mes)
        if df is None or df.empty:
            df = generate_synthetic_sih(uf, ano, mes)

        df_filtrado = filtrar_por_cid(df, cid_prefixos)

        if df_filtrado.empty:
            log.info(f"{uf} {ano}/{mes:02d}: nenhum registro após filtro, nada gravado.")
            return 0

        write_to_bronze(df_filtrado, uf, ano, mes)
        return len(df_filtrado)
    except Exception as e:  # noqa: BLE001 - garante que o lote inteiro não pare por 1 mês
        log.error(f"FALHA no mês {uf} {ano}/{mes:02d}, pulando para o próximo. Erro: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uf", "--UF", dest="uf", default="SP")

    # modo "um mês só" (compatibilidade com versões antigas do script)
    parser.add_argument("--ano", "--ANO", dest="ano", type=int, default=None)
    parser.add_argument("--mes", "--MES", dest="mes", type=int, default=None)

    # modo "intervalo de meses" — AGORA É O PADRÃO: 2020-01 até 2026-07
    # (agosto/2026 ainda não fechou no momento em que este script foi escrito)
    parser.add_argument("--ano_inicio", type=int, default=2020)
    parser.add_argument("--mes_inicio", type=int, default=1)
    parser.add_argument("--ano_fim", type=int, default=2026)
    parser.add_argument("--mes_fim", type=int, default=7)

    # filtro de doença (opcional). Por padrão NÃO filtra — o Bronze fica cru,
    # a escolha de quais doenças/períodos usar fica para Silver/Gold.
    parser.add_argument("--cid_prefixo", default=None,
                         help="Prefixos de CID-10 separados por vírgula (ex.: 'A90,A91' "
                              "para dengue). Padrão: sem filtro, traz tudo cru para o Bronze.")

    args = parser.parse_args()
    cid_prefixos = [c.strip() for c in args.cid_prefixo.split(",")] if args.cid_prefixo else None

    if args.ano is not None:
        # MODO MÊS ÚNICO explícito (só entra aqui se --ano foi passado na mão)
        mes = args.mes or 1
        log.info(f"Modo mês único: UF={args.uf} {args.ano}/{mes:02d}")
        processar_mes(args.uf, args.ano, mes, cid_prefixos)
        return

    # MODO INTERVALO (padrão): 2020-01 até 2026-07, sem filtro de doença
    meses = gerar_intervalo_meses(args.ano_inicio, args.mes_inicio, args.ano_fim, args.mes_fim)
    log.info(f"Ingerindo {len(meses)} meses ({meses[0][0]}-{meses[0][1]:02d} até "
              f"{meses[-1][0]}-{meses[-1][1]:02d}) para UF={args.uf}"
              + (f", filtrando CID {cid_prefixos}" if cid_prefixos else ", SEM filtro (Bronze cru)"))

    total = 0
    falhas = 0
    for i, (ano, mes) in enumerate(meses, start=1):
        qtd = processar_mes(args.uf, ano, mes, cid_prefixos)
        total += qtd
        if qtd == 0:
            falhas += 1
        if i % 12 == 0 or i == len(meses):
            log.info(f"Progresso: {i}/{len(meses)} meses processados "
                      f"({total} registros gravados até agora).")

    log.info(f"CONCLUÍDO: {total} registros gravados no total, UF={args.uf}, "
              f"período {meses[0][0]}-{meses[0][1]:02d} a {meses[-1][0]}-{meses[-1][1]:02d} "
              f"({len(meses)} meses, {falhas} vazios/com falha).")


if __name__ == "__main__":
    main()
