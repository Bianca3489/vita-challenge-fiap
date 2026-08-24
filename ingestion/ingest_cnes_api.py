"""
ingest_cnes_api.py
===================
FONTE 2 (JSON semiestruturado via API) do desafio: CNES — Cadastro Nacional
de Estabelecimentos de Saúde. Hospitais, leitos, UBS, tipologia, contatos.

Estratégia:
  1) Tenta consumir a API pública de Dados Abertos do Ministério da Saúde
     (apidadosabertos.saude.gov.br/cnes/estabelecimentos).
  2) Se a API estiver indisponível/mudar contrato, cai para um gerador
     sintético com o mesmo formato JSON.

⚠️ IMPORTANTE (descoberto testando com dado real): a API real não parece
respeitar de forma confiável o parâmetro `uf` na query string — em um teste
pedindo uf=SP, vieram estabelecimentos do Rio de Janeiro (codigo_uf=33).
Por isso este script SEMPRE valida no lado do cliente, descartando qualquer
registro cujo `codigo_uf` não bata com a UF pedida, e pagina (usando
`offset`) até juntar uma amostra decente ou desistir depois de N tentativas.

Também: o schema REAL da API é bem diferente do que documentação informal
sugere — os nomes de campo usados aqui (`codigo_uf`, `codigo_municipio`,
`latitude_estabelecimento_decimo_grau`, `codigo_tipo_unidade`, etc.) foram
confirmados batendo uma chamada real e inspecionando a resposta.

Saída: grava o JSON bruto (como veio da API, sem achatar) em
  s3://<DATALAKE_BUCKET>/bronze/cnes/uf=<UF>/cnes_<UF>_<timestamp>.json
"""
import argparse
import io
import json
import logging
import os
import random
import time
from datetime import datetime

import boto3
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest_cnes_api")

DATALAKE_BUCKET = os.environ.get("DATALAKE_BUCKET", "vita-datalake-<CONTA>")
CNES_API_BASE = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos"

# Código IBGE de UF (2 dígitos) — usado para VALIDAR (não confiar cegamente
# no filtro da API) e como plano B de parâmetro de busca.
UF_PARA_CODIGO_IBGE = {
    "AC": 12, "AL": 17, "AP": 16, "AM": 13, "BA": 29, "CE": 23, "DF": 53,
    "ES": 32, "GO": 52, "MA": 21, "MT": 51, "MS": 50, "MG": 31, "PA": 15,
    "PB": 25, "PR": 41, "PE": 26, "PI": 22, "RJ": 33, "RN": 24, "RS": 43,
    "RO": 11, "RR": 14, "SC": 42, "SP": 35, "SE": 28, "TO": 17,
}

MUNICIPIOS_UF_FALLBACK = {
    "SP": [("São Paulo", 3550308), ("Campinas", 3509502), ("Santos", 3548500)],
    "RJ": [("Rio de Janeiro", 3304557), ("Niterói", 3303302)],
    "CE": [("Fortaleza", 2304400), ("Sobral", 2312908)],
    "RS": [("Porto Alegre", 4314902)],
}

IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"


def _buscar_lista_municipios_reais(uf: str) -> list[tuple[str, int]]:
    """Busca a lista completa de municípios reais da UF no IBGE, pra usar
    como distribuição geográfica realista nos hospitais sintéticos (fallback
    de quando a API do CNES não encontrou o hospital pelo código exato).

    Sem isso, os hospitais sintéticos ficavam concentrados só em 3-4 cidades
    hardcoded, distorcendo qualquer análise geográfica no dashboard mesmo
    com o cadastro de município (municipios_referencia) tendo o dado real
    completo — o problema não era lá, era aqui."""
    try:
        resp = requests.get(IBGE_MUNICIPIOS_URL.format(uf=uf), timeout=20)
        resp.raise_for_status()
        dados = resp.json()
        municipios = [(m["nome"], m["id"]) for m in dados if "nome" in m and "id" in m]
        if municipios:
            log.info(f"{len(municipios)} municípios reais de {uf} carregados do IBGE "
                      f"para distribuição geográfica dos hospitais sintéticos.")
            return municipios
    except Exception as e:  # noqa: BLE001
        log.warning(f"Não consegui buscar municípios reais do IBGE ({e}). "
                     f"Usando lista pequena de fallback (poucas cidades).")
    return MUNICIPIOS_UF_FALLBACK.get(uf, [("Município Exemplo", 9999999)])


def _codigo_uf_do_registro(registro: dict) -> int | None:
    """Extrai o código IBGE de UF de um registro, tolerando pequenas variações
    de schema entre versões da API."""
    valor = registro.get("codigo_uf")
    if valor is None:
        return None
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None


def buscar_por_codigos_especificos(codigos_cnes: list[int], uf_esperada: str) -> list[dict]:
    """Busca cada estabelecimento pelo código CNES exato (em vez de amostra
    aleatória). Usado quando já sabemos, a partir do SIH, exatamente quais
    hospitais precisamos cadastrar — garante 100% de cobertura em vez de
    torcer pra amostra aleatória bater.

    Se a API não aceitar busca por código específico ou falhar pra algum
    código, esse código fica de fora (não trava o lote todo) e vira
    sintético depois, se for o caso.
    """
    codigo_ibge_esperado = UF_PARA_CODIGO_IBGE.get(uf_esperada.upper())
    encontrados = []
    nao_encontrados = []

    for i, codigo in enumerate(codigos_cnes, start=1):
        try:
            # Tenta primeiro o padrão REST de buscar 1 item específico pela URL
            # (.../estabelecimentos/2079895) — mais comum em APIs REST do que
            # passar como query param. Se der 404, marca como não encontrado
            # direto (sem contar como erro genérico).
            url_especifico = f"{CNES_API_BASE}/{codigo}"
            resp = requests.get(url_especifico, timeout=15)

            if resp.status_code == 404:
                nao_encontrados.append(codigo)
                continue
            resp.raise_for_status()
            payload = resp.json()

            # a resposta de "1 item específico" pode vir como objeto único,
            # ou ainda embrulhada numa lista/chave -- cobre os formatos comuns
            if isinstance(payload, dict) and "codigo_cnes" in payload:
                registro = payload
            elif isinstance(payload, dict):
                lista = payload.get("estabelecimentos") or payload.get("data")
                registro = lista[0] if lista else None
            elif isinstance(payload, list) and payload:
                registro = payload[0]
            else:
                registro = None

            if registro is None:
                nao_encontrados.append(codigo)
                continue

            # mesmo aqui, valida a UF -- não confia cegamente
            codigo_uf_real = _codigo_uf_do_registro(registro)
            if codigo_ibge_esperado is None or codigo_uf_real == codigo_ibge_esperado:
                encontrados.append(registro)
            else:
                nao_encontrados.append(codigo)
        except Exception:  # noqa: BLE001
            nao_encontrados.append(codigo)

        if i % 100 == 0:
            log.info(f"Progresso: {i}/{len(codigos_cnes)} códigos consultados "
                      f"({len(encontrados)} encontrados até agora).")
        time.sleep(0.15)  # gentileza com a API pública

    log.info(f"Busca direcionada concluída: {len(encontrados)} de {len(codigos_cnes)} "
              f"códigos encontrados na API real ({len(nao_encontrados)} não encontrados "
              f"ou de outra UF).")
    return encontrados, nao_encontrados


def try_download_cnes_api(uf: str, quantidade_desejada: int = 100,
                           max_paginas: int = 20) -> list[dict] | None:
    """Tenta baixar estabelecimentos reais da API pública do Ministério da
    Saúde, paginando até juntar `quantidade_desejada` registros que
    REALMENTE pertencem à UF pedida (valida client-side, não confia só no
    parâmetro da API). Retorna None se a API falhar ou não achar nada válido."""
    codigo_ibge_esperado = UF_PARA_CODIGO_IBGE.get(uf.upper())
    if codigo_ibge_esperado is None:
        log.warning(f"UF '{uf}' não reconhecida no mapa IBGE. Pulando validação de UF.")

    registros_validos: list[dict] = []
    registros_rejeitados = 0
    offset = 0
    limit = 50
    pagina = 0

    try:
        for pagina in range(max_paginas):
            params = {"uf": uf, "codigo_uf": codigo_ibge_esperado, "limit": limit, "offset": offset}
            resp = requests.get(CNES_API_BASE, params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            pagina_registros = payload.get("estabelecimentos") or payload.get("data") or payload
            if not isinstance(pagina_registros, list) or not pagina_registros:
                break  # acabaram os resultados

            for r in pagina_registros:
                codigo_uf_real = _codigo_uf_do_registro(r)
                if codigo_ibge_esperado is not None and codigo_uf_real != codigo_ibge_esperado:
                    registros_rejeitados += 1
                    continue
                registros_validos.append(r)

            offset += limit
            if len(registros_validos) >= quantidade_desejada:
                break
            time.sleep(0.3)  # gentileza com a API pública

        if registros_rejeitados > 0:
            log.warning(
                f"A API retornou {registros_rejeitados} registro(s) de OUTRA UF "
                f"mesmo pedindo uf={uf} — o filtro server-side não é confiável. "
                f"Validação client-side descartou esses registros."
            )

        if not registros_validos:
            log.warning(
                f"Nenhum estabelecimento REAL de {uf} (codigo_uf={codigo_ibge_esperado}) "
                f"foi encontrado após {pagina + 1} página(s). Usando fallback sintético."
            )
            return None

        log.info(f"{len(registros_validos)} estabelecimentos reais e validados "
                  f"de {uf} obtidos da API CNES.")
        return registros_validos

    except Exception as e:  # noqa: BLE001
        log.warning(f"API CNES indisponível ou falhou ({e}). Usando fallback sintético.")
        return None


def generate_synthetic_cnes(uf: str, n_estabelecimentos: int = 40) -> list[dict]:
    """Gera uma amostra sintética de estabelecimentos, no MESMO schema real
    confirmado (codigo_uf, codigo_municipio, latitude_estabelecimento_decimo_grau,
    codigo_tipo_unidade, etc.) — não no schema simplificado antigo."""
    codigo_uf = UF_PARA_CODIGO_IBGE.get(uf.upper(), 35)
    municipios = _buscar_lista_municipios_reais(uf)
    # tipos de unidade comuns no CNES (código real): 05=Hospital Geral,
    # 07=Hospital Especializado, 02=UBS/Posto de Saúde, 73=Pronto Socorro
    tipos_unidade = [5, 7, 2, 73]

    registros = []
    for i in range(n_estabelecimentos):
        nome_municipio, codigo_municipio = random.choice(municipios)
        eh_hospital = random.random() < 0.4
        registros.append({
            "codigo_cnes": 2000000 + i,
            "nome_fantasia": f"Hospital {random.choice(['Regional', 'Municipal', 'São', 'Santa'])} "
                              f"{random.choice(['Maria', 'Vicente', 'Messejana', 'Cariri', 'Sobral'])}"
                              if eh_hospital else f"UBS {nome_municipio} {i}",
            "codigo_tipo_unidade": random.choice([5, 7]) if eh_hospital else random.choice(tipos_unidade),
            "codigo_uf": codigo_uf,
            "codigo_municipio": codigo_municipio,
            "latitude_estabelecimento_decimo_grau": round(-23.5 + random.uniform(-3, 3), 6),
            "longitude_estabelecimento_decimo_grau": round(-46.6 + random.uniform(-3, 3), 6),
            "estabelecimento_possui_atendimento_hospitalar": 1 if eh_hospital else 0,
            "numero_telefone_estabelecimento":
                f"({random.randint(11, 99)}) 3{random.randint(1000,9999)}-{random.randint(1000,9999)}",
            "data_atualizacao": datetime.now().strftime("%Y-%m-%d"),
        })
    log.info(f"Gerados {len(registros)} estabelecimentos sintéticos de CNES para {uf}")
    return registros


def write_to_bronze(registros: list[dict], uf: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    key = f"bronze/cnes/uf={uf}/cnes_{uf}_{timestamp}.json"
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in registros)  # JSON Lines

    s3 = boto3.client("s3")
    try:
        s3.upload_fileobj(io.BytesIO(body.encode("utf-8")), DATALAKE_BUCKET, key)
        uri = f"s3://{DATALAKE_BUCKET}/{key}"
        log.info(f"Gravado no Bronze: {uri} ({len(registros)} registros)")
        return uri
    except Exception as e:  # noqa: BLE001
        local_path = os.path.join("output_local", key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(body)
        log.warning(f"Falha ao subir no S3 ({e}). Salvo localmente em {local_path}")
        return local_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uf", "--UF", dest="uf", default="SP")
    parser.add_argument("--quantidade", type=int, default=100,
                         help="Quantos estabelecimentos reais tentar juntar (padrão: 100)")
    parser.add_argument("--max_paginas", type=int, default=20,
                         help="Quantas páginas de 50 registros varrer no máximo "
                              "(a API parece ignorar o filtro de UF, então o "
                              "aproveitamento é baixo — ~15-20%% para SP). Padrão: 20 "
                              "(até 1000 candidatos varridos).")
    parser.add_argument("--codigos_csv", default=None,
                         help="Caminho de um CSV com uma coluna 'cnes_id' contendo "
                              "os códigos CNES exatos a buscar (ex.: exportado do "
                              "Athena com 'SELECT DISTINCT cnes_id FROM "
                              "vita_gold.fato_internacoes'). Quando informado, IGNORA "
                              "--quantidade/--max_paginas e busca cada código "
                              "individualmente — garante 100%% de cobertura em vez "
                              "de amostra aleatória.")
    args = parser.parse_args()

    if args.codigos_csv:
        import pandas as pd
        df_codigos = pd.read_csv(args.codigos_csv)
        coluna = "cnes_id" if "cnes_id" in df_codigos.columns else df_codigos.columns[0]
        codigos = df_codigos[coluna].dropna().astype(int).unique().tolist()
        log.info(f"Buscando {len(codigos)} códigos CNES específicos de {args.codigos_csv}")

        registros, nao_encontrados = buscar_por_codigos_especificos(codigos, args.uf)

        if nao_encontrados:
            log.info(f"Gerando dados sintéticos para os {len(nao_encontrados)} códigos "
                      f"não encontrados na API real (mantendo os MESMOS códigos CNES, "
                      f"pra garantir 100% de cobertura no JOIN com fato_internacoes).")
            sinteticos = generate_synthetic_cnes(args.uf, n_estabelecimentos=len(nao_encontrados))
            # substitui os codigo_cnes gerados aleatoriamente pelos códigos REAIS que faltam
            for registro_sintetico, codigo_real in zip(sinteticos, nao_encontrados):
                registro_sintetico["codigo_cnes"] = codigo_real
            registros.extend(sinteticos)
    else:
        registros = try_download_cnes_api(args.uf, quantidade_desejada=args.quantidade,
                                           max_paginas=args.max_paginas)
        if not registros:
            registros = generate_synthetic_cnes(args.uf)

    write_to_bronze(registros, args.uf)


if __name__ == "__main__":
    main()
