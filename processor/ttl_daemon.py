#!/usr/bin/env python3
# ttl_daemon.py - Демон для автоматической очистки

import time
import schedule
from ttl_cleaner import TTLManager

class TTLCleanupDaemon:
    def __init__(self, collections_to_clean):
        self.manager = TTLManager()
        self.collections = collections_to_clean
    
    def cleanup_job(self):
        """Задача очистки для всех коллекций"""
        print(f"\n🕒 [{time.ctime()}] Running TTL cleanup...")
        
        for collection in self.collections:
            expired_count = self.manager.get_expired_count(collection)
            if expired_count > 0:
                print(f"🗑️  Cleaning {collection}: {expired_count} expired points")
                self.manager.delete_expired_points(collection)
            else:
                print(f"✅ {collection}: no expired points")
    
    def run(self, cleanup_interval_hours=6):
        """Запуск демона"""
        print(f"🚀 TTL Cleanup Daemon started")
        print(f"📁 Monitoring collections: {self.collections}")
        print(f"⏰ Cleanup interval: every {cleanup_interval_hours} hours")
        
        # Настраиваем расписание
        schedule.every(cleanup_interval_hours).hours.do(self.cleanup_job)
        
        # Запускаем сразу первую очистку
        self.cleanup_job()
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Проверяем каждую минуту
        except KeyboardInterrupt:
            print("\n🛑 TTL Daemon stopped")

if __name__ == "__main__":
    # Коллекции для мониторинга
    collections = ["logs-ttl-7d", "logs-ttl-30d", "application-logs"]
    
    daemon = TTLCleanupDaemon(collections)
    daemon.run(cleanup_interval_hours=6)  # Очистка каждые 6 часов
