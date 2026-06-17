#   What alerts.py needs to do:
#   - One function: send_alert(symbol, anomaly_type, z_score, window_start)
#   - Inside: build a JSON payload and POST it to the webhook URL
#   - Discord expects: {"content": "your message here"}
#   - Use the requests library

from requests import post
import os

def send_alert(symbol, anomaly_type, z_score, window_start):
    webhook_url = os.environ["DISCORD_WEBHOOK_URL"]
    payload = {
        "content": f"Symbol: {symbol}\nAnomaly Type: {anomaly_type}\nZ-Score: {z_score}\nWindow Start: {window_start}"
    }
    post(webhook_url, json=payload)
    print("Alert sent!")
    return

