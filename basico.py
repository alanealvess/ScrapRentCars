import requests
import pandas as pd
from fuzzywuzzy import process, fuzz
import unicodedata

# Função para normalizar strings
def normalize(text):
    if pd.isna(text):
        return ""
    text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('ASCII')
    return ''.join(e for e in text.lower() if e.isalnum() or e.isspace()).strip()

# Carregar mapeamento externo
mapping_path = "vehicle_mappings.csv"
df_mapping = pd.read_csv(mapping_path, sep=";")

# URL da requisição
url = "https://www.viajanet.com.br/chapu/results?site=BR&channel=viajanet-site&channelType=WHITE_LABEL&language=PT&trackerid=500d0628-756f-4d82-8d06-28756f8d8286&pickupGid=CIT_6322&dropoffGid=CIT_6322&pickup_date=2025-06-01T11:00&dropoff_date=2025-06-07T11:00&filtersCameEncoded=false&webview=false&useNewFilters=false&pageviewId=cars-gui-9aeb0d54-9a44-4c42-9544-fb45ac1f4f1b&h=61892b54ba8d27d7ba38708d8afcf737&incomeType=UNKNOWN&domainName=www.viajanet.com.br&searchMode=FIRST_SEARCH&page=1&pageSize=15&referer=https://www.viajanet.com.br/carros&providerDespegar=false&categoryLimit=3"

# Cabeçalhos HTTP
headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://www.viajanet.com.br/cars/shop/city/REC/2025-06-01T11:00/city/REC/2025-06-07T11:00",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "X-Referrer": "https://www.viajanet.com.br/cars/shop/city/REC/2025-06-01T11:00/city/REC/2025-06-07T11:00",
    "X-UOW": "cars-gui-acd71d6f0cf68b40af822425-5458b94dbd-2n2ff-1747323061309",
    "X-guiVersion": "17.5.29",
    "X-trackingCode": "6818"
}

# Cookies essenciais
cookies = {
    "trackerid": "500d0628-756f-4d82-8d06-28756f8d8286",
    "trackeame_cookie": '{"id":"500d0628-756f-4d82-8d06-28756f8d8286","upa_id":"500d0628-756f-4d82-8d06-28756f8d8286","creation_date":"2025-05-15T15:30:49Z","company_id":"3212","version":"7.0"}',
    "datadome": "fonEFUzqDy65d358_7pQgA5Ot0La81ZcdjF8HCELdazqP6Of6o~MKYbfviKSxO5wVarfmz0X4PlH2o0GplEH9YJ117o9Va7Xm_7TMhJaJ_He8r2geSsSJRs4z6c~RFBs"
}

# Requisição
response = requests.get(url, headers=headers, cookies=cookies)

if response.status_code == 200:
    data = response.json()

    if "offers" in data:
        df_raw = pd.json_normalize(data["offers"])

        df = pd.DataFrame()
        df["vehicleName"] = df_raw.get("vehicle.model", "")
        df["rentalCompany"] = df_raw.get("carProviderCode", "")
        df["rentalPrice"] = df_raw.get("pricesDetail.BRL.daily.amount", "")
        df["gearType"] = df_raw.get("vehicle.specification.transmission.text", "")
        df["hasAirConditioning"] = df_raw.get("vehicle.specification.airConditioning.value", "")
        df["categoryName"] = df_raw.get("categoryCode", "")
        df["ratingPercent"] = ""
        df["retiradaDate"] = ""
        df["devolucaoDate"] = ""
        df["tierRange"] = ""
        df["codigo_asa"] = ""
        df["letra"] = ""

        # Normalizar os modelos do CSV em tempo de execução
        df_mapping["modelo_normalizado_temp"] = df_mapping["modelo_consulta"].apply(normalize)

        vehicle_names = df["vehicleName"].dropna().unique()
        name_to_model = {}

        for name in vehicle_names:
            nome_normalizado = normalize(name)
            match, score, _ = process.extractOne(
                nome_normalizado,
                df_mapping["modelo_normalizado_temp"],
                scorer=fuzz.token_set_ratio
            )
            modelo = df_mapping.loc[df_mapping["modelo_normalizado_temp"] == match, "modelo_consulta"]
            name_to_model[name] = modelo.iloc[0] if score >= 60 and not modelo.empty else None

        df["modelo_consulta"] = df["vehicleName"].map(name_to_model)
        df["vehicleName"] = df["modelo_consulta"]

        # Juntar com mapeamento
        df = df.merge(df_mapping, on="modelo_consulta", how="left")

        # Preencher colunas finais
        df["codigo_asa"] = df["codigo_asa_y"]
        df["letra"] = df["letra_y"]

        # Remover colunas auxiliares
        df = df.drop(columns=["modelo_consulta", "codigo_asa_y", "letra_y", "modelo_normalizado_temp"], errors="ignore")

        # Reorganizar colunas na ordem certa
        final_columns = [
            "vehicleName", "rentalCompany", "rentalPrice", "gearType", "hasAirConditioning",
            "categoryName", "ratingPercent", "retiradaDate", "devolucaoDate",
            "tierRange", "codigo_asa", "letra"
        ]
        df = df[final_columns]

        df.to_excel("viajanet_formatado.xlsx", index=False)
        print("✅ Arquivo 'viajanet_formatado.xlsx' criado com sucesso.")
    else:
        print("⚠️ JSON retornado não contém 'offers'.")
else:
    print(f"❌ Erro na requisição: {response.status_code}")
