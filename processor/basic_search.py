#!/usr/bin/env python3
import sys
import json
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

class LogSearchClient:
    def __init__(self, host="localhost", port=6333, collection_name="universal-logs"):
        self.client = QdrantClient(host=host, port=port)
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.collection_name = collection_name
    
    def semantic_search(self, query, limit=10, min_score=0.3, filters=None):
        """Семантический поиск по логам"""
        # Преобразуем запрос в вектор
        query_vector = self.model.encode(query).tolist()
        
        # Строим фильтр если указан
        search_filter = None
        if filters:
            filter_conditions = []
            if filters.get('level'):
                filter_conditions.append(
                    FieldCondition(key="level", match=MatchValue(value=filters['level']))
                )
            if filters.get('source'):
                filter_conditions.append(
                    FieldCondition(key="source", match=MatchValue(value=filters['source']))
                )
            if filter_conditions:
                search_filter = Filter(must=filter_conditions)
        
        # Выполняем поиск
        results = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            query_filter=search_filter,
            limit=limit,
            with_payload=True,
            score_threshold=min_score
        )
        
        return results
    
    def print_results(self, results, query):
        """Красивый вывод результатов"""
        print(f"\n🔍 Результаты поиска: '{query}'")
        print("=" * 80)
        
        if not results:
            print("❌ Ничего не найдено")
            return
        
        for i, result in enumerate(results, 1):
            payload = result.payload
            score = result.score
            
            # Цвета для уровней логов
            level_colors = {
                "ERROR": "🔴",
                "WARN": "🟡", 
                "INFO": "🔵",
                "DEBUG": "⚪"
            }
            level_icon = level_colors.get(payload.get('level', 'INFO'), '⚪')
            
            print(f"\n{i}. {level_icon} [{payload.get('level', 'UNKNOWN')}]")
            print(f"   📊 Схожесть: {score:.3f}")
            print(f"   📝 Сообщение: {payload.get('message', 'N/A')}")
            print(f"   🕒 Время: {payload.get('timestamp', 'N/A')}")
            print(f"   📍 Источник: {payload.get('source', 'unknown')}")
            print(f"   🆔 ID: {result.id}")
            print("-" * 60)

    def interactive_search(self):
        """Интерактивный режим поиска"""
        print("🚀 Qdrant Log Search Client")
        print("📁 Коллекция:", self.collection_name)
        print("💡 Примеры запросов: 'ошибки базы данных', 'медленные запросы', 'проблемы с памятью'")
        print("❌ Введите 'quit' для выхода\n")
        
        while True:
            try:
                query = input("\n🎯 Введите запрос: ").strip()
                
                if query.lower() in ['quit', 'exit', 'q']:
                    print("👋 До свидания!")
                    break
                
                if not query:
                    continue
                
                # Опции фильтрации
                print("\n⚙️  Дополнительные фильтры (нажмите Enter чтобы пропустить):")
                level_filter = input("   Уровень (ERROR/WARN/INFO/DEBUG): ").strip().upper()
                source_filter = input("   Источник: ").strip()
                
                filters = {}
                if level_filter and level_filter in ['ERROR', 'WARN', 'INFO', 'DEBUG']:
                    filters['level'] = level_filter
                if source_filter:
                    filters['source'] = source_filter
                
                # Выполняем поиск
                results = self.semantic_search(query, limit=8, filters=filters if filters else None)
                self.print_results(results, query)
                
            except KeyboardInterrupt:
                print("\n👋 До свидания!")
                break
            except Exception as e:
                print(f"❌ Ошибка: {e}")

def main():
    # Можно указать коллекцию через аргумент командной строки
    collection_name = sys.argv[1] if len(sys.argv) > 1 else "universal-logs"
    
    client = LogSearchClient(collection_name=collection_name)
    client.interactive_search()

if __name__ == "__main__":
    main()
