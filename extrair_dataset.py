"""
Extração e decodificação do dataset SINAN (dengue, chikungunya, zika, febre amarela)
para um único arquivo Parquet local -- sem MongoDB, sem notebook, sem dependência de rede
além do download dos CSVs originais.

Uso:
    python extrair_dataset.py

Saída:
    casos_arboviroses.parquet  (no mesmo diretório do script)
"""
import gc
import io
import zipfile

import numpy as np
import pandas as pd
import requests

# --------------------------------------------------------------------------
# Configuração de volume -- ajuste estes números se o arquivo final ainda
# ficar grande demais para o seu disco/RAM. Um cap menor = arquivo menor,
# mas ainda representativo (amostra aleatória, não corte por região/período).
# --------------------------------------------------------------------------
ANOS_BUSCA = range(2022, 2027)
CAP_DENGUE_CHIK = None  # sem cap -- censo completo, nao amostra
CAP_ZIKA = None         # sem cap -- censo completo, nao amostra
RANDOM_STATE = 42
CAMINHO_SAIDA = "casos_arboviroses.parquet"

np.random.seed(RANDOM_STATE)

# --------------------------------------------------------------------------
# Mapeamentos (portados 1:1 do notebook de análise -- não reimplementados)
# --------------------------------------------------------------------------
COMORBIDADES = {
    "DIABETES": "diabetes",
    "HIPERTENSA": "hipertensao",
    "HEPATOPAT": "hepatopatia",
    "RENAL": "doenca_renal_cronica",
    "HEMATOLOG": "doenca_hematologica",
    "ACIDO_PEPT": "doenca_acido_peptica",
    "AUTO_IMUNE": "doenca_autoimune",
}

SEXO_MAP = {"M": "masculino", "F": "feminino", "I": "ignorado"}

CLASSI_FIN_MAP_DENGCHIK = {
    5: "descartado", 8: "inconclusivo",
    10: "dengue", 11: "dengue_sinais_alarme", 12: "dengue_grave", 13: "chikungunya",
    1: "dengue", 2: "dengue_complicacoes", 3: "fhd", 4: "scd",
}
GRAVIDADE_ORDINAL = {
    "dengue": "leve", "chikungunya": "leve",
    "dengue_sinais_alarme": "alarme", "dengue_complicacoes": "alarme",
    "dengue_grave": "grave", "fhd": "grave", "scd": "grave",
}
CLASSI_FIN_MAP_ZIKA = {1: "confirmado", 2: "descartado", 8: "inconclusivo"}

HOSPITALIZ_MAP = {1: True, 2: False}
EVOLUCAO_MAP = {1: "cura", 2: "obito_agravo", 3: "obito_outras", 4: "obito_investig", 9: "ignorado"}
OBITO_EVOLUCAO = {"obito_agravo", "obito_outras", "obito_investig"}

ORDEM_FAIXAS = ["<1", "1-4", "5-11", "12-19", "20-39", "40-59", "60+"]

COLUNAS_DENGCHIK = [
    "NU_ANO", "SG_UF_NOT", "DT_SIN_PRI", "DT_NOTIFIC", "NU_IDADE_N", "CS_SEXO",
    "DIABETES", "HEMATOLOG", "HEPATOPAT", "RENAL", "HIPERTENSA", "ACIDO_PEPT", "AUTO_IMUNE",
    "CLASSI_FIN", "HOSPITALIZ", "DT_INTERNA", "EVOLUCAO", "DT_OBITO",
]
COLUNAS_ZIKA = [
    "NU_ANO", "SG_UF_NOT", "DT_SIN_PRI", "DT_NOTIFIC", "NU_IDADE_N", "CS_SEXO",
    "CLASSI_FIN", "EVOLUCAO", "DT_OBITO",
]

BASE_S3 = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br"
PASTA_S3 = {"dengue": "SINAN/Dengue/csv", "chikungunya": "SINAN/Chikungunya/csv", "zika": "SINAN/Zikavirus/csv"}
PREFIXO_ARQ = {"dengue": "DENGBR", "chikungunya": "CHIKBR", "zika": "ZIKABR"}


# --------------------------------------------------------------------------
# UF via API do IBGE
# --------------------------------------------------------------------------
def obter_mapa_uf_ibge() -> dict:
    resp = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/estados", timeout=30)
    resp.raise_for_status()
    return {str(item["id"]): item["sigla"] for item in resp.json()}


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------
def baixar_csv_sinan(doenca: str, ano: int, colunas: list) -> pd.DataFrame:
    ano_curto = str(ano)[2:]
    url = f"{BASE_S3}/{PASTA_S3[doenca]}/{PREFIXO_ARQ[doenca]}{ano_curto}.csv.zip"
    nome_csv = f"{PREFIXO_ARQ[doenca]}{ano_curto}.csv"

    resp = requests.get(url, timeout=120)
    if resp.status_code != 200:
        print(f"  [{doenca} {ano}] arquivo indisponivel ({resp.status_code}), pulando.")
        return pd.DataFrame(columns=colunas)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open(nome_csv) as f:
            cabecalho = pd.read_csv(f, nrows=0).columns
        colunas_validas = [c for c in colunas if c in cabecalho]
        faltantes = sorted(set(colunas) - set(colunas_validas))
        with z.open(nome_csv) as f:
            df = pd.read_csv(f, usecols=colunas_validas, dtype=str, low_memory=False)
    for col in faltantes:
        df[col] = pd.NA
    print(f"  [{doenca} {ano}] {len(df):,} linhas carregadas"
          + (f" (colunas ausentes: {faltantes})" if faltantes else ""))
    return df


def baixar_febre_amarela() -> pd.DataFrame:
    url = f"{BASE_S3}/Febre+Amarela/fa_casoshumanos_1994-2026.csv"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    df = pd.read_csv(io.BytesIO(resp.content), sep=";", encoding="latin1", low_memory=False)
    df = df[pd.to_numeric(df["ANO_IS"], errors="coerce").between(min(ANOS_BUSCA), max(ANOS_BUSCA))]
    print(f"  [febre_amarela] {len(df):,} linhas no intervalo {min(ANOS_BUSCA)}-{max(ANOS_BUSCA)}")
    return df


# --------------------------------------------------------------------------
# Decodificação (portada 1:1 do notebook de análise)
# --------------------------------------------------------------------------
def to_int_code(serie: pd.Series) -> pd.Series:
    return pd.to_numeric(serie, errors="coerce").astype("Int64")


def decodificar_idade(nu_idade_n: pd.Series) -> pd.Series:
    s = pd.to_numeric(nu_idade_n, errors="coerce")
    unidade = (s // 1000).astype("Int64")
    valor = (s % 1000).astype("Int64")
    fator = unidade.map({1: 1 / (365 * 24), 2: 1 / 365, 3: 1 / 12, 4: 1.0})
    return (valor.astype("float") * fator).round(2)


def faixa_etaria(idade_anos: pd.Series) -> pd.Series:
    return pd.cut(idade_anos, bins=[-0.01, 1, 5, 12, 19, 39, 59, 200], labels=ORDEM_FAIXAS)


def flag_comorbidade(codigo: pd.Series) -> pd.Series:
    return codigo.map({1: "sim", 2: "nao", 9: "ignorado"}).fillna("ignorado")


def janela_dias(fim: pd.Series, inicio: pd.Series, limite: int = 60) -> pd.Series:
    d = (fim - inicio).dt.days
    return d.where((d >= 0) & (d <= limite))


def coluna_data(df_raw: pd.DataFrame, nome: str) -> pd.Series:
    if nome in df_raw.columns:
        return pd.to_datetime(df_raw[nome], errors="coerce")
    return pd.Series(pd.NaT, index=df_raw.index)


# --------------------------------------------------------------------------
# Construção do dataframe FLAT (uma coluna booleana por comorbidade,
# em vez de subdocumento aninhado -- muito mais direto pra groupby em pandas)
# --------------------------------------------------------------------------
def construir_dataframe_arbovirose(df_raw: pd.DataFrame, doenca: str) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame()

    tem_comorb = doenca in ("dengue", "chikungunya")
    idade = decodificar_idade(df_raw["NU_IDADE_N"])
    faixa = faixa_etaria(idade)
    sexo = df_raw["CS_SEXO"].map(SEXO_MAP).fillna("ignorado")
    uf = df_raw["SG_UF_NOT"].map(MAPA_UF_IBGE).fillna(df_raw["SG_UF_NOT"])
    ano = to_int_code(df_raw["NU_ANO"])

    classi = to_int_code(df_raw["CLASSI_FIN"])
    if tem_comorb:
        classificacao = classi.map(CLASSI_FIN_MAP_DENGCHIK)
        gravidade = classificacao.map(GRAVIDADE_ORDINAL)
    else:
        classificacao = classi.map(CLASSI_FIN_MAP_ZIKA)
        gravidade = pd.Series([None] * len(df_raw), index=df_raw.index)

    hospitalizado = (to_int_code(df_raw["HOSPITALIZ"]).map(HOSPITALIZ_MAP)
                     if "HOSPITALIZ" in df_raw.columns else pd.Series([None] * len(df_raw), index=df_raw.index))

    evolucao = to_int_code(df_raw["EVOLUCAO"]).map(EVOLUCAO_MAP)
    obito = evolucao.isin(OBITO_EVOLUCAO)

    dt_sintoma = coluna_data(df_raw, "DT_SIN_PRI")
    dt_interna = coluna_data(df_raw, "DT_INTERNA")
    atraso_dias = janela_dias(dt_interna, dt_sintoma) if tem_comorb else pd.Series([np.nan] * len(df_raw), index=df_raw.index)
    atraso_proxy = "sintoma->internacao" if tem_comorb else None

    grave_ou_internado = ((gravidade == "grave") | (hospitalizado == True)  # noqa: E712
                          if tem_comorb else pd.Series([None] * len(df_raw), index=df_raw.index))

    df_out = pd.DataFrame({
        "doenca": doenca,
        "uf": uf.to_numpy(),
        "ano": ano.astype("float"),
        "idade": idade.astype("float"),
        "faixa_etaria": faixa.astype(object),
        "sexo": sexo.to_numpy(),
        "atraso_dias": atraso_dias.astype("float"),
        "atraso_proxy": atraso_proxy,
        "classificacao": classificacao.astype(object),
        "gravidade": gravidade.astype(object) if tem_comorb else None,
        "hospitalizado": hospitalizado.astype(object) if tem_comorb else None,
        "grave_ou_internado": grave_ou_internado.astype(object) if tem_comorb else None,
        "obito": obito.to_numpy(),
    })

    # comorbidades: uma coluna booleana por tipo (vetorizado, sem loop linha a linha)
    if tem_comorb:
        n_comorbidades = np.zeros(len(df_raw), dtype="float")
        for coluna_raw, rotulo in COMORBIDADES.items():
            presente = (flag_comorbidade(to_int_code(df_raw[coluna_raw])) == "sim").to_numpy()
            df_out[rotulo] = presente
            n_comorbidades += presente.astype(int)
        df_out["n_comorbidades"] = n_comorbidades
    else:
        for rotulo in COMORBIDADES.values():
            df_out[rotulo] = None
        df_out["n_comorbidades"] = None

    return df_out


def construir_dataframe_febre_amarela(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame()

    idade = pd.to_numeric(df_raw["IDADE"], errors="coerce")
    faixa = faixa_etaria(idade)
    sexo = df_raw["SEXO"].map(SEXO_MAP).fillna("ignorado")
    uf = df_raw["UF_LPI"].map(MAPA_UF_IBGE).fillna(df_raw["UF_LPI"])
    ano = pd.to_numeric(df_raw["ANO_IS"], errors="coerce")

    dt_is = pd.to_datetime(df_raw["DT_IS"], format="%d/%m/%Y", errors="coerce")
    dt_obito = pd.to_datetime(df_raw["DT_OBITO"], format="%d/%m/%Y", errors="coerce")
    atraso_dias = janela_dias(dt_obito, dt_is)

    obito = (df_raw["OBITO"].astype(str).str.upper().str.strip()
             .map({"SIM": True, "NAO": False, "NÃO": False}))

    df_out = pd.DataFrame({
        "doenca": "febre_amarela",
        "uf": uf.to_numpy(),
        "ano": ano.astype("float"),
        "idade": idade.astype("float"),
        "faixa_etaria": faixa.astype(object),
        "sexo": sexo.to_numpy(),
        "atraso_dias": atraso_dias.astype("float"),
        "atraso_proxy": "sintoma->obito",
        "classificacao": None,
        "gravidade": None,
        "hospitalizado": None,
        "grave_ou_internado": None,
        "obito": obito.to_numpy(),
        "n_comorbidades": None,
    })
    for rotulo in COMORBIDADES.values():
        df_out[rotulo] = None
    return df_out


# --------------------------------------------------------------------------
# Execução principal
# --------------------------------------------------------------------------
def main():
    global MAPA_UF_IBGE
    print("Buscando mapa de UF via API do IBGE...")
    MAPA_UF_IBGE = obter_mapa_uf_ibge()
    print(f"{len(MAPA_UF_IBGE)} UFs carregadas.\n")

    pedacos = []
    CAPS = {"dengue": CAP_DENGUE_CHIK, "chikungunya": CAP_DENGUE_CHIK, "zika": CAP_ZIKA}
    COLUNAS_POR_DOENCA = {"dengue": COLUNAS_DENGCHIK, "chikungunya": COLUNAS_DENGCHIK, "zika": COLUNAS_ZIKA}

    for doenca, colunas in COLUNAS_POR_DOENCA.items():
        for ano in ANOS_BUSCA:
            print(f"\n=== {doenca} {ano} ===")
            try:
                df_raw = baixar_csv_sinan(doenca, ano, colunas)
                if df_raw.empty:
                    continue
                df_raw = df_raw[to_int_code(df_raw["NU_ANO"]) == ano]

                cap = CAPS[doenca]
                if cap is not None and len(df_raw) > cap:
                    print(f"  [amostragem] {len(df_raw):,} linhas > cap de {cap:,}; amostrando.")
                    df_raw = df_raw.sample(n=cap, random_state=RANDOM_STATE)

                df_pedaco = construir_dataframe_arbovirose(df_raw, doenca)
                pedacos.append(df_pedaco)
                print(f"  -> {len(df_pedaco):,} linhas processadas")

                del df_raw, df_pedaco
                gc.collect()
            except Exception as e:
                print(f"  [erro] {doenca} {ano}: {e}")

    print("\n=== febre amarela ===")
    try:
        df_fa = baixar_febre_amarela()
        df_fa_pronto = construir_dataframe_febre_amarela(df_fa)
        pedacos.append(df_fa_pronto)
        print(f"  -> {len(df_fa_pronto):,} linhas processadas")
        del df_fa, df_fa_pronto
        gc.collect()
    except Exception as e:
        print(f"  [erro] febre_amarela: {e}")

    print("\nConsolidando e salvando em Parquet...")
    df_final = pd.concat(pedacos, ignore_index=True)
    del pedacos
    gc.collect()

    df_final.to_parquet(CAMINHO_SAIDA, index=False, compression="snappy")

    import os
    tamanho_mb = os.path.getsize(CAMINHO_SAIDA) / 1024 / 1024
    print(f"\nArquivo salvo: {CAMINHO_SAIDA}")
    print(f"Tamanho: {tamanho_mb:.1f} MB")
    print(f"Total de linhas: {len(df_final):,}")
    print("\nResumo por doenca:")
    print(df_final.groupby("doenca").size())


if __name__ == "__main__":
    main()