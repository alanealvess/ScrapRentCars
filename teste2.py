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
data_inicial = datetime(2026, 1, 9)
dias = 13
tiers = [2, 6, 13, 15]
chamadas = 0  # contador de chamadas
horarios = ["T18:00", "T19:00", "T20:00", "T21:00", "T22:00", "T23:00"]

# ========= FUNÇÕES AUXILIARES =========
def normalize(text):
    if pd.isna(text):
        return ""
    text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('ASCII')
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return ' '.join(text.lower().split())

def get_chapu_response(driver):
    logs = driver.get_log("performance")
    for log in logs:
        try:
            message = json.loads(log["message"])["message"]
            if message["method"] == "Network.responseReceived":
                params = message["params"]
                url = params["response"]["url"]
                if "/chapu/results" in url and params["response"]["mimeType"] == "application/json":
                    request_id = params["requestId"]
                    body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                    return body.get("body")
        except Exception:
            continue
    return None

def iniciar_driver():
    caps = DesiredCapabilities.CHROME
    caps["goog:loggingPrefs"] = {"performance": "ALL"}
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    return uc.Chrome(options=options, desired_capabilities=caps)

# ========= PREPARAÇÃO =========
df_mapping = pd.read_csv("vehicle_mappings.csv", sep=";")
df_mapping["modelo_normalizado_temp"] = df_mapping["modelo_consulta"].apply(normalize)

rental_map = pd.read_csv("rental_mappings.csv")
category_map = pd.read_csv("category_mappings.csv")

dfs = []
driver = iniciar_driver()

# ========= LOOP =========
base_url = "https://www.viajanet.com.br/cars/shop/city/{city}/{pickup}/city/{city}/{dropoff}"

for i in range(dias):
    retirada = data_inicial + timedelta(days=i)

    random_tiers = tiers[:]
    random.shuffle(random_tiers)

    for t in random_tiers:
        devolucao = retirada + timedelta(days=t)

        horario_escolhido = random.choice(horarios)
        r_str = retirada.strftime(f"%Y-%m-%d{horario_escolhido}")
        d_str = devolucao.strftime(f"%Y-%m-%d{horario_escolhido}")

        url = base_url.format(city=city, pickup=r_str, dropoff=d_str)

        chamadas += 1
        if chamadas % 10 == 0:
            print("🔁 Reinicializando navegador para evitar detecção...")
            driver.quit()
            time.sleep(3)
            driver = iniciar_driver()

        print(f"🔎 Acessando: {url}")
        driver.execute_cdp_cmd("Network.enable", {})
        driver.get(url)

        # ====== Rolagem realista (nova) ======
        for _ in range(random.randint(1, 3)):
            scroll_value = random.randint(100, 800)
            driver.execute_script(f"window.scrollBy(0, {scroll_value})")
            time.sleep(random.uniform(0.5, 1.2))

        # ====== Espera variada ======
        time.sleep(random.uniform(8, 14))

        resposta_json = get_chapu_response(driver)
        if not resposta_json:
            print(f"⚠️ Nenhuma resposta capturada para {r_str} → {d_str}")
            continue

        try:
            data = json.loads(resposta_json)
            if "offers" not in data or not data["offers"]:
                print(f"⚠️ Sem ofertas para {r_str} → {d_str}")
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

            print(f"✅ Ofertas processadas: {len(df)}")

        except Exception as e:
            print(f"❌ Erro ao processar dados para {r_str}: {e}")

# ========= FINAL =========
driver.quit()

if dfs:
    df_final = pd.concat(dfs, ignore_index=True)
    nome_arquivo = f"viajanet_{city}_{data_inicial.strftime('%Y%m%d')}_formatado.xlsx"
    df_final.to_excel(nome_arquivo, index=False)
    print(f"✅ Planilha '{nome_arquivo}' criada com sucesso.")
else:
    print("⚠️ Nenhuma oferta válida processada.")
