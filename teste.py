import json
import os
import time
from datetime import datetime, timedelta
import undetected_chromedriver as uc
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# Configurações
base_url = "https://www.viajanet.com.br/cars/shop/city/REC/{pickup}/city/REC/{dropoff}"
data_inicial = datetime(2025, 6, 1)
tiers = [3, 5]
dias = 3

# Setup
os.makedirs("responses", exist_ok=True)
caps = DesiredCapabilities.CHROME
caps["goog:loggingPrefs"] = {"performance": "ALL"}
options = uc.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")

driver = uc.Chrome(options=options, desired_capabilities=caps)

def get_chapu_response():
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
                    return url, body.get("body")
        except Exception:
            continue
    return None, None

# Loop
for i in range(dias):
    retirada = data_inicial + timedelta(days=i)
    for t in tiers:
        devolucao = retirada + timedelta(days=t)
        r_str = retirada.strftime("%Y-%m-%dT11:00")
        d_str = devolucao.strftime("%Y-%m-%dT11:00")
        url = base_url.format(pickup=r_str, dropoff=d_str)

        print(f"🔎 Acessando: {url}")
        driver.execute_cdp_cmd("Network.enable", {})
        driver.get(url)
        time.sleep(10)

        chapu_url, resposta_json = get_chapu_response()
        if resposta_json:
            nome = f"responses/REC_{retirada.strftime('%Y%m%d')}_{devolucao.strftime('%Y%m%d')}.json"
            with open(nome, "w", encoding="utf-8") as f:
                f.write(resposta_json)
            print(f"✅ Resposta capturada: {chapu_url}")
            print(f"💾 Salvo como: {nome}")
        else:
            print("⚠️ Resposta /chapu/results não capturada.")

driver.quit()
