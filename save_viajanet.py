from mitmproxy import http
import json
from datetime import datetime
import os

os.makedirs("capturas", exist_ok=True)

def response(flow: http.HTTPFlow):
    if "/chapu/results" in flow.request.pretty_url:
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"capturas/viajanet_{now}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump({
                "url": flow.request.pretty_url,
                "headers": dict(flow.request.headers),
                "cookies": flow.request.cookies.fields,
                "response": flow.response.text
            }, f, indent=2)
        print(f"💾 Requisição salva em {filename}")
