import time
import re
import pandas as pd
import unicodedata
from datetime import datetime, timedelta
from fuzzywuzzy import process, fuzz
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ========= CONFIGURAÇÕES =========
city_name = "Fortaleza"
# 110 = Fortaleza; 178 = Recife
city_code = 110
data_inicial = datetime(2025, 6, 1)
dias = 15
tiers = [2, 6, 13, 15]
hora_padrao = 11

# ========= FUNÇÕES =========
def normalize(text):
    if pd.isna(text):
        return ""
    text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('ASCII')
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return text.lower().strip()

def montar_url(city_code, retirada, devolucao):
    ts_retirada = int(retirada.timestamp())
    ts_devolucao = int(devolucao.timestamp())
    return f"https://www.rentcars.com/pt-br/reserva/listar/{city_code}-{ts_retirada}-{city_code}-{ts_devolucao}-0-0-0-0-0-0-0-0"

def extrair_dados_da_pagina(driver):
    cards = driver.find_elements(By.CLASS_NAME, "card-vehicle-container_1eavC5yY")
    dados = []
    for card in cards:
        try:
            nome = card.find_element(By.CLASS_NAME, "card-vehicle-title_1x3XzWOV").text.strip()
            preco = card.find_element(By.CLASS_NAME, "total-amount_1XUQg1Kt").text.strip()

            # Locadora
            try:
                locadora = card.find_element(By.CSS_SELECTOR, ".rental-company-evaluation-img_3FvMRZD5 img").get_attribute("alt")
            except:
                locadora = "Não informado"

            # Avaliação
            try:
                avaliacao = card.find_element(By.CLASS_NAME, "evaluation-value_gQkFUU98").text.strip()
            except:
                avaliacao = ""

            # Gear Type e Ar-condicionado
            gear_type = ""
            has_ac = ""
            try:
                config_items = card.find_elements(By.CLASS_NAME, "booking-configurations__item--description")
                for item in config_items:
                    icone = item.find_element(By.XPATH, "..//i").get_attribute("class")
                    if "icon-cambio" in icone:
                        gear_type = item.text.strip()
                    if "icon-ac" in icone:
                        has_ac = "true"
            except:
                pass

            dados.append((nome, preco, locadora, gear_type, has_ac, avaliacao))
        except:
            continue
    return dados

# ========= MAPEAMENTOS =========
df_mapping = pd.read_csv("vehicle_mappings.csv", sep=";")
df_mapping["modelo_normalizado_temp"] = df_mapping["modelo_consulta"].apply(normalize)

rental_map = pd.read_csv("rental_mappings.csv")
category_map = pd.read_csv("category_mappings.csv")

dfs = []

# ========= LOOP PRINCIPAL =========
for i in range(dias):
    retirada = data_inicial + timedelta(days=i)
    for t in tiers:
        devolucao = retirada + timedelta(days=t)
        retirada_dt = retirada.replace(hour=hora_padrao)
        devolucao_dt = devolucao.replace(hour=hora_padrao)

        print(f"📡 Coletando dados: {retirada_dt} → {devolucao_dt}")
        url = montar_url(city_code, retirada_dt, devolucao_dt)

        options = uc.ChromeOptions()
        options.add_argument("--window-size=1920,1080")
        driver = uc.Chrome(options=options)
        driver.get(url)

        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'carros encontrados')]"))
            )

            # Scroll dinâmico
            last_height = driver.execute_script("return document.body.scrollHeight")
            for _ in range(15):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2.5)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            dados = extrair_dados_da_pagina(driver)
        except Exception as e:
            print(f"⚠️ Erro durante carregamento: {e}")
            driver.quit()
            continue

        driver.quit()

        if not dados:
            print("⚠️ Nenhum dado encontrado.")
            continue

        df = pd.DataFrame(dados, columns=[
            "vehicleName", "rentalPrice", "rentalCompany", "gearType", "hasAirConditioning", "ratingPercent"
        ])
        df["categoryName"] = ""
        df["codigo_asa"] = ""
        df["letra"] = ""

        # Mapeamento de modelos
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

        # Locadora
        df = df.merge(rental_map, left_on="rentalCompany", right_on="codigo", how="left")
        df["rentalCompany"] = df["nome_locadora"].combine_first(df["rentalCompany"])
        df = df.drop(columns=["codigo", "nome_locadora"], errors="ignore")

        # Categoria
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
        print(f"✅ Registros capturados: {len(df)}")

# ========= SALVAR EXCEL =========
if dfs:
    df_final = pd.concat(dfs, ignore_index=True)
    nome_arquivo = f"rentcars_{city_name.lower()}_{data_inicial.strftime('%Y%m%d')}_formatado.xlsx"
    df_final.to_excel(nome_arquivo, index=False)
    print(f"\n📁 Planilha gerada: {nome_arquivo}")
else:
    print("⚠️ Nenhum dado foi coletado.")
