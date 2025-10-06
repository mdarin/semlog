#!/usr/bin/env python3
import sys
import time
import re
from datetime import datetime
from threading import Timer
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient, models

class UniversalLogProcessor:
    def __init__(self, collection_name="universal-logs"):
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.client = QdrantClient("localhost")
        self.collection_name = collection_name
        
        # Инициализируем коллекцию если её нет
        self.init_collection()
        
        # Конфигурация батчинга
        self.batch_size = 15
        self.batch_timeout = 3  # секунды
        self.batch_buffer = []
        self.flush_timer = None
        self.reset_timer()
        
        # Счетчики для мониторинга
        self.processed_count = 0
        self.start_time = time.time()
    
    def init_collection(self):
        """Создаем коллекцию если не существует"""
        try:
            self.client.get_collection(self.collection_name)
        except Exception:
            # Коллекция не существует, создаем
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=384,  # all-MiniLM-L6-v2 dimension
                    distance=models.Distance.COSINE
                )
            )
            print(f"✅ Created collection: {self.collection_name}")
    
    def reset_timer(self):
        """Сбрасываем таймер для принудительной отправки батча"""
        if self.flush_timer:
            self.flush_timer.cancel()
        self.flush_timer = Timer(self.batch_timeout, self.flush_batch)
        self.flush_timer.start()
    
    def extract_log_metadata(self, line):
        """Извлекаем метаданные из строки лога"""
        # Паттерны для различных форматов логов
        patterns = [
            # JSON логи: {"timestamp": "...", "level": "ERROR", "message": "..."}
            (r'^{.*"level"\s*:\s*"(\w+)".*"message"\s*:\s*"([^"]+)".*}$', self.parse_json_log),
            
            # Стандартные логи: [INFO] 2024-01-15 message
            (r'^\[(\w+)\]\s+(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2})\s+(.+)$', self.parse_bracket_log),
            
            # Nginx/Apache: 192.168.1.1 - - [15/Jan/2024:10:30:00] "GET /"
            (r'^(\d+\.\d+\.\d+\.\d+).*\[(.+?)\].*"(GET|POST|PUT|DELETE)', self.parse_web_log),
        ]
        
        for pattern, parser in patterns:
            match = re.match(pattern, line.strip())
            if match:
                return parser(match, line)
        
        # Fallback: простой текст
        return {
            "message": line.strip(),
            "level": self.detect_log_level(line),
            "timestamp": datetime.now().isoformat(),
            "source": "stdin",
            "format": "plain"
        }
    
    def parse_json_log(self, match, original_line):
        """Парсим JSON логи"""
        try:
            data = json.loads(original_line)
            return {
                "message": data.get("message", original_line),
                "level": data.get("level", "INFO"),
                "timestamp": data.get("timestamp", datetime.now().isoformat()),
                "source": data.get("source", "unknown"),
                "format": "json"
            }
        except:
            return self.fallback_parse(original_line)
    
    def parse_bracket_log(self, match, original_line):
        """Парсим логи в формате [LEVEL] timestamp message"""
        return {
            "message": match.group(3),
            "level": match.group(1),
            "timestamp": match.group(2),
            "source": "application", 
            "format": "bracketed"
        }
    
    def parse_web_log(self, match, original_line):
        """Парсим веб-логи"""
        return {
            "message": original_line.strip(),
            "level": "INFO",  # web logs обычно INFO
            "timestamp": match.group(2),
            "source": "web_server",
            "client_ip": match.group(1),
            "format": "web"
        }
    
    def fallback_parse(self, line):
        """Fallback парсинг для неизвестных форматов"""
        return {
            "message": line.strip(),
            "level": self.detect_log_level(line),
            "timestamp": datetime.now().isoformat(), 
            "source": "unknown",
            "format": "plain"
        }
    
    def detect_log_level(self, message):
        """Авто-определение уровня лога"""
        message_lower = message.lower()
        
        error_keywords = ['error', 'exception', 'failed', 'fatal', 'crash', 'panic']
        warn_keywords = ['warn', 'warning', 'deprecated', 'slow', 'timeout']
        debug_keywords = ['debug', 'trace', 'verbose']
        
        if any(word in message_lower for word in error_keywords):
            return "ERROR"
        elif any(word in message_lower for word in warn_keywords):
            return "WARN" 
        elif any(word in message_lower for word in debug_keywords):
            return "DEBUG"
        else:
            return "INFO"
    
    def process_line(self, line):
        """Обработка одной строки из stdin"""
        if not line.strip():
            return
            
        try:
            # Извлекаем метаданные
            log_data = self.extract_log_metadata(line)
            self.batch_buffer.append(log_data)
            
            # Обновляем таймер
            self.reset_timer()
            
            # Проверяем размер батча
            if len(self.batch_buffer) >= self.batch_size:
                self.flush_batch()
                
            self.processed_count += 1
            
            # Периодический статус
            if self.processed_count % 100 == 0:
                elapsed = time.time() - self.start_time
                rate = self.processed_count / elapsed
                print(f"📊 Processed {self.processed_count} logs ({rate:.1f}/sec)", 
                      file=sys.stderr)
                      
        except Exception as e:
            print(f"❌ Error processing line: {e}", file=sys.stderr)
    
    def flush_batch(self):
        """Отправка накопленного батча в Qdrant"""
        if not self.batch_buffer:
            return
            
        try:
            # Создаем эмбеддинги для всех сообщений в батче
            messages = [log["message"] for log in self.batch_buffer]
            embeddings = self.model.encode(messages)
            
            # Подготавливаем точки для Qdrant
            points = []
            for i, (log, embedding) in enumerate(zip(self.batch_buffer, embeddings)):
                point_id = int(time.time() * 1000000) + i  # microsecond precision
                
                points.append(models.PointStruct(
                    id=point_id,
                    vector=embedding.tolist(),
                    payload={
                        "message": log["message"],
                        "level": log["level"],
                        "timestamp": log["timestamp"],
                        "source": log["source"],
                        "format": log["format"],
                        "processed_at": datetime.now().isoformat(),
                        "batch_size": len(self.batch_buffer)
                    }
                ))
            
            # Сохраняем в Qdrant
            self.client.upsert(
                collection_name=self.collection_name,
                points=points
            )
            
            print(f"✅ Saved {len(points)} logs to {self.collection_name}", 
                  file=sys.stderr)
            
        except Exception as e:
            print(f"❌ Batch flush error: {e}", file=sys.stderr)
        finally:
            self.batch_buffer.clear()
    
    def run(self):
        """Основной цикл обработки stdin"""
        print(f"🚀 Universal Log Processor started", file=sys.stderr)
        print(f"📁 Collection: {self.collection_name}", file=sys.stderr)
        print(f"⚙️  Batch: {self.batch_size} logs or {self.batch_timeout}s", file=sys.stderr)
        print("---", file=sys.stderr)
        
        try:
            for line in sys.stdin:
                self.process_line(line)
                
        except KeyboardInterrupt:
            print("\n🛑 Shutdown signal received...", file=sys.stderr)
        except BrokenPipeError:
            print("💥 Input pipe broken", file=sys.stderr)
        except Exception as e:
            print(f"💥 Fatal error: {e}", file=sys.stderr)
        finally:
            # Гарантированно сохраняем оставшиеся логи
            if self.flush_timer:
                self.flush_timer.cancel()
            if self.batch_buffer:
                print(f"💾 Flushing {len(self.batch_buffer)} remaining logs...", 
                      file=sys.stderr)
                self.flush_batch()
            
            elapsed = time.time() - self.start_time
            print(f"👋 Processor stopped. Stats:", file=sys.stderr)
            print(f"   Total processed: {self.processed_count} logs", file=sys.stderr)
            print(f"   Duration: {elapsed:.1f} seconds", file=sys.stderr)
            print(f"   Rate: {self.processed_count/elapsed:.1f} logs/sec", file=sys.stderr)

if __name__ == "__main__":
    # Можно указать имя коллекции через аргумент
    collection_name = sys.argv[1] if len(sys.argv) > 1 else "universal-logs"
    processor = UniversalLogProcessor(collection_name)
    processor.run()
