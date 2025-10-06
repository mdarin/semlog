#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timedelta
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

class AdvancedLogSearchClient:
    def __init__(self, host="localhost", port=6333):
        self.client = QdrantClient(host=host, port=port)
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
    
    def search_logs(self, query, collection_name="universal-logs", 
                   limit=10, min_score=0.3, level=None, source=None, hours=None):
        """Расширенный поиск с фильтрами по времени"""
        
        # Строим фильтры
        filter_conditions = []
        
        if level:
            filter_conditions.append(
                FieldCondition(key="level", match=MatchValue(value=level))
            )
        
        if source:
            filter_conditions.append(
                FieldCondition(key="source", match=MatchValue(value=source))
            )
        
        if hours:
            time_threshold = (datetime.now() - timedelta(hours=hours)).isoformat()
            filter_conditions.append(
                FieldCondition(
                    key="timestamp",
                    range=Range(gte=time_threshold)
                )
            )
        
        search_filter = Filter(must=filter_conditions) if filter_conditions else None
        
        # Поиск
        query_vector = self.model.encode(query).tolist()
        results = self.client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=search_filter,
            limit=limit,
            with_payload=True,
            score_threshold=min_score
        )
        
        return results
    
    def get_collection_stats(self, collection_name):
        """Статистика коллекции"""
        try:
            collection_info = self.client.get_collection(collection_name)
            count_result = self.client.count(collection_name)
            
            return {
                "collection": collection_name,
                "vectors_count": count_result.count,
                "status": collection_info.status,
                "vectors_config": collection_info.config.params.vectors
            }
        except Exception as e:
            return {"error": str(e)}
    
    def find_similar_logs(self, log_id, collection_name="universal-logs", limit=5):
        """Поиск похожих логов по ID"""
        try:
            # Получаем вектор по ID
            points = self.client.retrieve(
                collection_name=collection_name,
                ids=[log_id],
                with_vectors=True
            )
            
            if not points:
                return []
            
            point = points[0]
            similar_results = self.client.search(
                collection_name=collection_name,
                query_vector=point.vector,
                limit=limit + 1,  # +1 потому что найдет сам себя
                with_payload=True
            )
            
            # Исключаем исходный лог из результатов
            return [r for r in similar_results if r.id != log_id]
            
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            return []
    
    def export_results(self, results, filename="search_results.json"):
        """Экспорт результатов в JSON"""
        export_data = []
        for result in results:
            export_data.append({
                "id": result.id,
                "score": result.score,
                "payload": result.payload
            })
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Результаты экспортированы в {filename}")

def main():
    parser = argparse.ArgumentParser(description="Qdrant Log Search Client")
    parser.add_argument("query", nargs="?", help="Поисковый запрос")
    parser.add_argument("--collection", "-c", default="universal-logs", 
                       help="Имя коллекции")
    parser.add_argument("--limit", "-l", type=int, default=10, 
                       help="Лимит результатов")
    parser.add_argument("--level", choices=["ERROR", "WARN", "INFO", "DEBUG"],
                       help="Фильтр по уровню")
    parser.add_argument("--source", help="Фильтр по источнику")
    parser.add_argument("--hours", type=int, help="Фильтр по времени (последние N часов)")
    parser.add_argument("--min-score", type=float, default=0.3,
                       help="Минимальная схожесть")
    parser.add_argument("--stats", action="store_true", 
                       help="Показать статистику коллекции")
    parser.add_argument("--similar-to", type=int, 
                       help="Найти похожие на лог с указанным ID")
    parser.add_argument("--export", help="Экспорт результатов в файл")
    
    args = parser.parse_args()
    client = AdvancedLogSearchClient()
    
    if args.stats:
        # Показать статистику
        stats = client.get_collection_stats(args.collection)
        print("📊 Статистика коллекции:")
        print(json.dumps(stats, indent=2, ensure_ascii=False))
        return
    
    if args.similar_to:
        # Поиск похожих логов
        results = client.find_similar_logs(args.similar_to, args.collection, args.limit)
        print(f"🔍 Логи похожие на ID {args.similar_to}:")
        for i, result in enumerate(results, 1):
            print(f"{i}. [{result.payload.get('level')}] {result.payload.get('message')}")
            print(f"   Схожесть: {result.score:.3f}, ID: {result.id}\n")
        return
    
    if not args.query:
        print("❌ Укажите поисковый запрос")
        parser.print_help()
        return
    
    # Выполняем поиск
    results = client.search_logs(
        query=args.query,
        collection_name=args.collection,
        limit=args.limit,
        min_score=args.min_score,
        level=args.level,
        source=args.source,
        hours=args.hours
    )
    
    # Вывод результатов
    print(f"\n🔍 Результаты поиска: '{args.query}'")
    print(f"📁 Коллекция: {args.collection}")
    print(f"📊 Найдено: {len(results)} результатов")
    print("=" * 80)
    
    for i, result in enumerate(results, 1):
        payload = result.payload
        print(f"{i}. [{payload.get('level', 'UNKNOWN')}] {payload.get('message')}")
        print(f"   📍 {payload.get('source', 'unknown')} | 🕒 {payload.get('timestamp', 'N/A')}")
        print(f"   🎯 Схожесть: {result.score:.3f} | 🆔 {result.id}")
        print("-" * 60)
    
    # Экспорт если нужно
    if args.export and results:
        client.export_results(results, args.export)

if __name__ == "__main__":
    main()
