import os
import json
import time
import re
import random
from datetime import datetime, timedelta
import pandas as pd
from fuzzywuzzy import process, fuzz
import unicodedata
import undetected_chromedriver as uc
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# ========= CONFIGURAÇÕES =========
city = "REC"
data_inicial = datetime(2025, 5, 21)
dias = 9
tiers = [2, 6, 13, 15]
horario = "T18:00"

# ========= FUNÇÕES AUXILIARES =========
def normalize(text):
    if pd.isna(text):
        return ""
    text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('ASCII')
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return text.lower().strip()

def capturar_requisicao(city, retirada, devolucao):
    url = f"https://www.viajanet.com.br/cars/shop/city/{city}/{retirada}/city/{city}/{devolucao}"
    caps = DesiredCapabilities.CHROME
    caps["goog:loggingPrefs"] = {"performance": "ALL"}
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = uc.Chrome(options=options, desired_capabilities=caps)
    driver.minimize_window()  # <-- Adicionado para rodar minimizado

    driver.execute_cdp_cmd("Network.enable", {})
    driver.get(url)
    time.sleep(5)

    chapu_data = None
    logs = driver.get_log("performance")

    for log in logs:
        try:
            msg = json.loads(log["message"])["message"]
            if msg["method"] == "Network.responseReceived":
                if "/chapu/results" in msg["params"]["response"]["url"]:
                    request_id = msg["params"]["requestId"]
                    response_body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                    chapu_data = json.loads(response_body["body"])
                    break
        except Exception:
            continue

    driver.quit()

    if not chapu_data:
        raise Exception("❌ Corpo da resposta /chapu/results não encontrado")

    return chapu_data

# ========= PREPARAÇÃO =========
df_mapping = pd.read_csv("vehicle_mappings.csv", sep=";")
df_mapping["modelo_normalizado_temp"] = df_mapping["modelo_consulta"].apply(normalize)

rental_map = pd.read_csv("rental_mappings.csv")
category_map = pd.read_csv("category_mappings.csv")

dfs = []

# ========= LOOP DE REQUISIÇÕES =========
for i in range(dias):
    retirada = data_inicial + timedelta(days=i)
    for t in tiers:
        devolucao = retirada + timedelta(days=t)
        r_str = retirada.strftime(f"%Y-%m-%d{horario}")
        d_str = devolucao.strftime(f"%Y-%m-%d{horario}")

        print(f"📡 Capturando dados: {r_str} → {d_str}")
        try:
            data = capturar_requisicao(city, r_str, d_str)
        except Exception as e:
            print(f"⚠️ Falha na requisição para {r_str} → {d_str}: {e}")
            continue

        if "offers" not in data or not data["offers"]:
            print("⚠️ Sem ofertas disponíveis.")
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
        df = df.drop(columns=["codigo", "nome_locadora"], errors="ignore")

        df = df.merge(category_map, left_on="categoryName", right_on="codigo", how="left")
        df["categoryName"] = df["nome_categoria"].combine_first(df["categoryName"])
        df = df.drop(columns=["codigo", "nome_categoria"], errors="ignore")

        df["retiradaDate"] = retirada.strftime("%Y-%m-%d")
        df["devolucaoDate"] = devolucao.strftime("%Y-%m-%d")
        df["tierRange"] = t
        df["codigo_asa"] = df["codigo_asa_y"].fillna("")
        df["letra"] = df["letra_y"].fillna("")
        df = df.drop(columns=["modelo_consulta", "codigo_asa_y", "letra_y"], errors="ignore")

        final_columns = [
            "vehicleName", "rentalCompany", "rentalPrice", "gearType", "hasAirConditioning",
            "categoryName", "ratingPercent", "retiradaDate", "devolucaoDate",
            "tierRange", "codigo_asa", "letra"
        ]
        df = df[final_columns]
        dfs.append(df)
        print(f"✅ Processado: {len(df)}")

# ========= SALVAR =========
if dfs:
    df_final = pd.concat(dfs, ignore_index=True)
    nome = f"viajanet_{city}_{data_inicial.strftime('%Y%m%d')}_formatado.xlsx"
    df_final.to_excel(nome, index=False)
    print(f"📁 Planilha gerada: {nome}")
else:
    print("⚠️ Nenhum dado coletado.")
