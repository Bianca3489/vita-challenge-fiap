"""
ml_indice_pressao_hospitalar.py
==================================
Versão STANDALONE (pandas + scikit-learn, sem Spark/Glue) do cálculo do
Índice de Pressão Hospitalar. Útil para:
  - testar a lógica rapidamente em notebook/local antes de rodar no Glue
  - gerar o print/gráfico que vai no PPTX de evidências (Sprint 2)

A lógica é idêntica à embutida em etl/glue_silver_to_gold_kpis.py.

Uso:
  python ml_indice_pressao_hospitalar.py --input gold_features.csv --output indice_pressao.csv
Se --input não for informado, gera uma amostra sintética para demonstração.
"""
import argparse

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

FEATURE_COLS = ["qtd_internacoes", "permanencia_media", "pacientes_criticos", "taxa_ocupacao_estimada"]
NIVEIS = ["Baixo risco", "Atenção", "Crítico", "Sobrecarga iminente"]


def synthetic_sample(n_hospitais: int = 40, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "cnes_id": np.arange(2000000, 2000000 + n_hospitais),
        "nome_estabelecimento": [f"Hospital {i}" for i in range(n_hospitais)],
        "qtd_internacoes": rng.integers(20, 900, size=n_hospitais),
        "permanencia_media": np.round(rng.gamma(2.0, 2.0, size=n_hospitais), 1),
        "pacientes_criticos": rng.integers(0, 60, size=n_hospitais),
        "taxa_ocupacao_estimada": np.round(rng.uniform(0.3, 1.1, size=n_hospitais), 2).clip(0, 1),
    })


def calcular_indice_pressao(df: pd.DataFrame, k: int = 4) -> pd.DataFrame:
    df = df.copy()
    df[FEATURE_COLS] = df[FEATURE_COLS].fillna(0)

    X = StandardScaler().fit_transform(df[FEATURE_COLS])
    k = min(k, max(1, df.shape[0]))
    kmeans = KMeans(n_clusters=k, n_init=10, random_state=42)
    df["cluster"] = kmeans.fit_predict(X)

    centro_por_cluster = df.groupby("cluster")["taxa_ocupacao_estimada"].mean().sort_values()
    ordem_risco = {cluster: rank for rank, cluster in enumerate(centro_por_cluster.index)}
    df["nivel_risco"] = df["cluster"].map(ordem_risco).map(lambda r: NIVEIS[min(r, len(NIVEIS) - 1)])

    perm_max = df["permanencia_media"].max() or 1
    criticos_max = df["pacientes_criticos"].max() or 1
    df["indice_pressao"] = (
        df["taxa_ocupacao_estimada"].clip(0, 1) * 60
        + (df["permanencia_media"] / perm_max) * 25
        + (df["pacientes_criticos"] / criticos_max) * 15
    ).round(1)

    return df.sort_values("indice_pressao", ascending=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default="indice_pressao_hospitalar.csv")
    args = parser.parse_args()

    df = pd.read_csv(args.input) if args.input else synthetic_sample()
    resultado = calcular_indice_pressao(df)
    resultado.to_csv(args.output, index=False)
    print(resultado[["nome_estabelecimento" if "nome_estabelecimento" in resultado else "cnes_id",
                      "indice_pressao", "nivel_risco"]].head(10).to_string(index=False))
    print(f"\nSalvo em {args.output}")


if __name__ == "__main__":
    main()
