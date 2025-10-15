from flask import Flask, jsonify, request
import os

# چون اسم فایل اصلیت build_hourly_performance.py هست:
from build_hourly_performance import build_hourly_performance

app = Flask(__name__)

@app.get("/")
def health():
    return jsonify(status="ok", service="hourly_performance")

@app.post("/run-hourly-performance")
def run_hourly_performance():
    # امن‌سازی ساده (اختیاری): اگر RUN_TOKEN ست بود باید در هدر بیاد
    expected = os.getenv("RUN_TOKEN")
    if expected:
        token = request.headers.get("X-Run-Token")
        if token != expected:
            return jsonify(ok=False, error="unauthorized"), 401

    try:
        build_hourly_performance()
        return jsonify(ok=True, message="Hourly_Performance built.")
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
