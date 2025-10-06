#!/usr/bin/env python3
# quick_search.py - Для интеграции в скрипты

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

def quick_search(query, collection="universal-logs", limit=5):
    """Быстрый поиск для использования в других скриптах"""
    client = QdrantClient("localhost")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    vector = model.encode(query).tolist()
    results = client.search(
        collection_name=collection,
        query_vector=vector,
        limit=limit,
        with_payload=True
    )
    
    return [{
        'message': r.payload.get('message'),
        'level': r.payload.get('level'),
        'score': r.score,
        'timestamp': r.payload.get('timestamp')
    } for r in results]

# Пример использования в других скриптах
if __name__ == "__main__":
    import sys
    query = sys.argv[1] if len(sys.argv) > 1 else "ошибки"
    results = quick_search(query)
    
    for r in results:
        print(f"{r['level']}: {r['message']} (score: {r['score']:.3f})")
