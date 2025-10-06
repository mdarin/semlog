#!/usr/bin/env python3
import sys
import time
import datetime
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient, models

class TTLEnabledLogProcessor:
    def __init__(self, collection_name="logs-ttl", ttl_days=7):
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.client = QdrantClient("localhost")
        self.collection_name = collection_name
        self.ttl_days = ttl_days
        
        # Инициализируем коллекцию с TTL
        self.init_collection_with_ttl()
        
        # Батчинг
        self.batch_size = 10
        self.batch_buffer = []
    
    def init_collection_with_ttl(self):
        """Создаем коллекцию и настраиваем TTL"""
        try:
            self.client.get_collection(self.collection_name)
            print(f"✅ Коллекция {self.collection_name} уже существует")
        except Exception:
            # Создаем новую коллекцию
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=384,
                    distance=models.Distance.COSINE
                )
            )
            
            # Создаем индекс для TTL поля
            self.client.create_payload_index(
                collection_name=self.collection_name,
                field_name="expires_at",
                field_schema=models.PayloadSchemaType.DATETIME
            )
            
            print(f"✅ Создана коллекция {self.collection_name} с TTL {self.ttl_days} дней")
    
    def calculate_expires_at(self):
        """Вычисляем время истечения TTL"""
        expires_at = datetime.datetime.now() + datetime.timedelta(days=self.ttl_days)
        return expires_at.isoformat()
    
    def process_line(self, line):
        """Обработка строки с добавлением TTL"""
        if not line.strip():
            return
            
        log_data = {
            "message": line.strip(),
            "level": self.detect_log_level(line),
            "timestamp": datetime.datetime.now().isoformat(),
            "source": "stdin",
            "expires_at": self.calculate_expires_at(),  # ✅ TTL поле
            "ttl_days": self.ttl_days
        }
        
        self.batch_buffer.append(log_data)
        
        if len(self.batch_buffer) >= self.batch_size:
            self.flush_batch()
    
    def flush_batch(self):
        """Отправка батча с TTL"""
        if not self.batch_buffer:
            return
            
        try:
            messages = [log["message"] for log in self.batch_buffer]
            embeddings = self.model.encode(messages)
            
            points = []
            for i, (log, embedding) in enumerate(zip(self.batch_buffer, embeddings)):
                points.append(models.PointStruct(
                    id=int(time.time() * 1000000) + i,
                    vector=embedding.tolist(),
                    payload=log  # ✅ Включаем expires_at в payload
                ))
            
            self.client.upsert(
                collection_name=self.collection_name,
                points=points
            )
            
            print(f"✅ Saved {len(points)} logs with TTL {self.ttl_days} days")
            
        except Exception as e:
            print(f"❌ Error: {e}", file=sys.stderr)
        finally:
            self.batch_buffer.clear()
    
    def detect_log_level(self, message):
        """Определение уровня лога"""
        message_lower = message.lower()
        if any(word in message_lower for word in ['error', 'exception', 'failed']):
            return "ERROR"
        elif any(word in message_lower for word in ['warn', 'warning']):
            return "WARN"
        elif any(word in message_lower for word in ['debug']):
            return "DEBUG"
        else:
            return "INFO"
    
    def run(self):
        """Запуск процессора"""
        print(f"🚀 TTL Log Processor started - TTL: {self.ttl_days} days")
        
        try:
            for line in sys.stdin:
                self.process_line(line)
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
        finally:
            if self.batch_buffer:
                self.flush_batch()

if __name__ == "__main__":
    # Можно указать TTL через аргументы
    import sys
    ttl_days = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    collection_name = sys.argv[2] if len(sys.argv) > 2 else f"logs-ttl-{ttl_days}d"
    
    processor = TTLEnabledLogProcessor(collection_name, ttl_days)
    processor.run()
