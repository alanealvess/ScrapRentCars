import requests
import json
import pandas as pd
import random
import re
import unicodedata
from datetime import datetime, timedelta
from fuzzywuzzy import process, fuzz

# ========== CONFIGURAÇÕES ==========
city = "REC"
pickup_gid = "CIT_6322"
dropoff_gid = "CIT_6322"
data_inicial = datetime(2026, 1, 19)
dias = 3
tiers = [2, 6, 13, 15]
horarios = ["T18:00", "T19:00", "T20:00", "T21:00", "T22:00", "T23:00"]

# ========== FUNÇÕES ==========
def normalize(text):
    if pd.isna(text):
        return ""
    text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('ASCII')
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return ' '.join(text.lower().split())

def construir_url(retirada, devolucao):
    base = "https://www.viajanet.com.br/chapu/results"
    return (
        f"{base}?site=BR&channel=viajanet-site&channelType=WHITE_LABEL&language=PT"
        f"&trackerid=345088cf-6845-4f89-9ad8-237974ab6218"
        f"&pickupGid={pickup_gid}&dropoffGid={dropoff_gid}"
        f"&pickup_date={retirada}&dropoff_date={devolucao}"
        f"&filtersCameEncoded=false&webview=false&useNewFilters=false"
        f"&pageviewId=cars-gui-fa1c24cc-fe2e-4d01-b427-311d781b59ad"
        f"&h=1919cdc36801c81ecd062ee290df1a98&incomeType=UNKNOWN"
        f"&domainName=www.viajanet.com.br&searchMode=FIRST_SEARCH"
        f"&page=1&pageSize=15&providerDespegar=false&categoryLimit=3"
    )

headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://www.viajanet.com.br/cars/shop/city/REC/2026-01-19T18:00/city/REC/2026-01-21T18:00",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "X-Referrer": "https://www.viajanet.com.br/cars/shop/city/REC/2026-01-19T18:00/city/REC/2026-01-21T18:00",
    "X-UOW": "cars-gui-fa1c24cc-fe2e-4d01-b427-311d781b59ad",
    "X-guiVersion": "17.5.32",
    "X-trackingCode": "6818"
}

cookies = {
    "trackerid": "345088cf-6845-4f89-9ad8-237974ab6218",
    "trackeame_cookie": '{"id":"345088cf-6845-4f89-9ad8-237974ab6218","upa_id":"345088cf-6845-4f89-9ad8-237974ab6218","creation_date":"2024-05-15T12:00:00Z","company_id":"3212","version":"7.0"}',
    "datadome": "fonEFUzqDy65d358_7pQgA5Ot0La81ZcdjF8HCELdazqP6Of6o~MKYbfviKSxO5wVarfmz0X4PlH2o0GplEH9YJ117o9Va7Xm_7TMhJaJ_He8r2geSsSJRs4z6c~RFBs"
}

# ========== MAPEAMENTOS ==========
df_mapping = pd.read_csv("vehicle_mappings.csv", sep=";")
df_mapping["modelo_normalizado_temp"] = df_mapping["modelo_consulta"].apply(normalize)

rental_map = pd.read_csv("rental_mappings.csv")
category_map = pd.read_csv("category_mappings.csv")

# ========== LOOP PRINCIPAL ==========
dfs = []

for i in range(dias):
    retirada_base = data_inicial + timedelta(days=i)
    random_tiers = tiers[:]
    random.shuffle(random_tiers)

    for tier in random_tiers:
        devolucao_base = retirada_base + timedelta(days=tier)
        horario = random.choice(horarios)
        retirada_str = retirada_base.strftime(f"%Y-%m-%d{horario}")
        devolucao_str = devolucao_base.strftime(f"%Y-%m-%d{horario}")

        print(f"🔎 Requisitando: {retirada_str} → {devolucao_str}")
        url = construir_url(retirada_str, devolucao_str)

        response = requests.get(url, headers=headers, cookies=cookies)

        if response.status_code != 200:
            print(f"❌ Erro HTTP {response.status_code}")
            continue

        data = response.json()
        if "offers" not in data or not data["offers"]:
            print("⚠️ Nenhuma oferta encontrada.")
            continue

        df_raw = pd.json_normalize(data["offers"])
        df = pd.DataFrame()
        df["vehicleName"] = df_raw.get("vehicle.model", "")
        df["rentalCompany"] = df_raw.get("carProviderCode", "")
        df["rentalPrice"] = df_raw.get("pricesDetail.BRL.daily.amount", "")
        df["gearType"] = df_raw.get("vehicle.specification.transmission.text", "")
        df["hasAirConditioning"] = df_raw.get("vehicle.specification.airConditioning.value", "")
        df["categoryName"] = df_raw.get("categoryCode", "")
        df["ratingPercent"] = ""
        df["codigo_asa"] = ""
        df["letra"] = ""

        name_to_model = {}
        for name in df["vehicleName"].dropna().unique():
            nome_normalizado = normalize(name)
            match, score, _ = process.extractOne(
                nome_normalizado, df_mapping["modelo_normalizado_temp"], scorer=fuzz.token_set_ratio
            )
            modelo = df_mapping.loc[df_mapping["modelo_normalizado_temp"] == match, "modelo_consulta"]
            name_to_model[name] = modelo.iloc[0] if score >= 50 and not modelo.empty else None

        df["modelo_consulta"] = df["vehicleName"].map(name_to_model)
        df = df.merge(df_mapping, on="modelo_consulta", how="left")
        df["vehicleName"] = df["modelo_consulta"].combine_first(df["vehicleName"])

        df = df.merge(rental_map, left_on="rentalCompany", right_on="codigo", how="left")
        df["rentalCompany"] = df["nome_locadora"].combine_first(df["rentalCompany"])
        df.drop(columns=["codigo", "nome_locadora"], inplace=True, errors="ignore")

        df = df.merge(category_map, left_on="categoryName", right_on="codigo", how="left")
        df["categoryName"] = df["nome_categoria"].combine_first(df["categoryName"])
        df.drop(columns=["codigo", "nome_categoria"], inplace=True, errors="ignore")

        df["retiradaDate"] = retirada_base.strftime("%Y-%m-%d")
        df["devolucaoDate"] = devolucao_base.strftime("%Y-%m-%d")
        df["tierRange"] = tier

        df["codigo_asa"] = df["codigo_asa_y"].fillna("")
        df["letra"] = df["letra_y"].fillna("")
        df.drop(columns=["modelo_consulta", "codigo_asa_y", "letra_y"], errors="ignore", inplace=True)

        final_cols = [
            "vehicleName", "rentalCompany", "rentalPrice", "gearType", "hasAirConditioning",
            "categoryName", "ratingPercent", "retiradaDate", "devolucaoDate",
            "tierRange", "codigo_asa", "letra"
        ]
        dfs.append(df[final_cols])

        print(f"✅ Ofertas coletadas: {len(df)}")

# ========== SALVAR ==========
if dfs:
    df_final = pd.concat(dfs, ignore_index=True)
    arquivo = f"viajanet_{city}_{data_inicial.strftime('%Y%m%d')}_formatado.xlsx"
    df_final.to_excel(arquivo, index=False)
    print(f"✅ Planilha '{arquivo}' criada com sucesso.")
else:
    print("⚠️ Nenhum dado para salvar.")
