#!/usr/bin/env python3
# monitor_processor.py - Мониторинг состояния процессора
import time
import requests
from datetime import datetime

def check_processor_health():
    """Проверка здоровья Qdrant и коллекций"""
    try:
        # Проверяем доступность Qdrant
        response = requests.get("http://localhost:6333/collections")
        if response.status_code == 200:
            collections = response.json().get('result', {}).get('collections', [])
            print(f"✅ Qdrant healthy. Collections: {[c['name'] for c in collections]}")
            return True
        else:
            print("❌ Qdrant not responding")
            return False
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False

if __name__ == "__main__":
    while True:
        print(f"\n[{datetime.now().isoformat()}] Health Check:")
        check_processor_health()
        time.sleep(30)
