import os
import json
import re
import pandas as pd
from datetime import datetime
from fuzzywuzzy import process, fuzz
import unicodedata

# ========= FUNÇÕES =========

def normalize(text):
    if pd.isna(text):
        return ""
    text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('ASCII')
    return ''.join(e for e in text.lower() if e.isalnum() or e.isspace()).strip()

def extrair_datas_do_nome_arquivo(nome_arquivo):
    padrao = r'REC_(\d{8})_(\d{8})'
    match = re.search(padrao, nome_arquivo)
    if match:
        retirada = datetime.strptime(match.group(1), "%Y%m%d")
        devolucao = datetime.strptime(match.group(2), "%Y%m%d")
        return retirada, devolucao
    return None, None

# ========= EXECUÇÃO =========

mapping_path = "vehicle_mappings.csv"
df_mapping = pd.read_csv(mapping_path, sep=";")
df_mapping["modelo_normalizado_temp"] = df_mapping["modelo_consulta"].apply(normalize)

dfs = []

# Processar todos os arquivos .json na pasta responses/
for filename in os.listdir("responses"):
    if filename.endswith(".json"):
        path = os.path.join("responses", filename)
        retiradaDate, devolucaoDate = extrair_datas_do_nome_arquivo(filename)

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if "offers" not in data or not data["offers"]:
                print(f"⚠️ Arquivo {filename} não contém ofertas.")
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

            # Mapeamento de modelos
            name_to_model = {}
            for name in df["vehicleName"].dropna().unique():
                nome_normalizado = normalize(name)
                match, score, _ = process.extractOne(
                    nome_normalizado, df_mapping["modelo_normalizado_temp"], scorer=fuzz.token_set_ratio
                )
                modelo = df_mapping.loc[df_mapping["modelo_normalizado_temp"] == match, "modelo_consulta"]
                name_to_model[name] = modelo.iloc[0] if score >= 60 and not modelo.empty else None

            df["modelo_consulta"] = df["vehicleName"].map(name_to_model)
            df["vehicleName"] = df["modelo_consulta"]
            df = df.merge(df_mapping, on="modelo_consulta", how="left")

            # Datas e tier
            if retiradaDate and devolucaoDate:
                df["retiradaDate"] = retiradaDate.strftime("%Y-%m-%d")
                df["devolucaoDate"] = devolucaoDate.strftime("%Y-%m-%d")
                df["tierRange"] = (devolucaoDate - retiradaDate).days
            else:
                df["tierRange"] = None

            df["codigo_asa"] = df["codigo_asa_y"]
            df["letra"] = df["letra_y"]
            df = df.drop(columns=["modelo_consulta", "codigo_asa_y", "letra_y"], errors="ignore")

            final_columns = [
                "vehicleName", "rentalCompany", "rentalPrice", "gearType", "hasAirConditioning",
                "categoryName", "ratingPercent", "retiradaDate", "devolucaoDate",
                "tierRange", "codigo_asa", "letra"
            ]
            df = df[final_columns]
            dfs.append(df)

        except Exception as e:
            print(f"❌ Erro ao processar {filename}: {e}")

# Concatenar
if dfs:
    df_final = pd.concat(dfs, ignore_index=True)
    df_final.to_excel("viajanet_formatado_json.xlsx", index=False)
    print("✅ Planilha 'viajanet_formatado_json.xlsx' criada com sucesso.")
else:
    print("⚠️ Nenhum arquivo JSON válido processado.")
