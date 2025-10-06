from sentence_transformers import SentenceTransformer
import qdrant_client
from qdrant_client.models import PointStruct, VectorParams, Distance
import time

def test_qdrant_connection():
    """Проверяем подключение к Qdrant"""
    try:
        # Пробуем разные варианты подключения
        clients = [
            qdrant_client.QdrantClient("localhost:6333"),
            qdrant_client.QdrantClient(":memory:"),  # In-memory
            qdrant_client.QdrantClient(host="localhost", port=6333),
            qdrant_client.QdrantClient()  # По умолчанию localhost:6333
        ]
        
        for client in clients:
            try:
                # Простая проверка - получаем список коллекций
                collections = client.get_collections()
                print(f"✅ Успешное подключение: {client._client.rest_uri}")
                return client
            except Exception as e:
                continue
                
        print("❌ Не удалось подключиться к Qdrant")
        print("🚀 Запустите Qdrant одним из способов:")
        print("   docker run -p 6333:6333 qdrant/qdrant")
        print("   Или используем in-memory режим...")
        
        # Используем in-memory как fallback
        return qdrant_client.QdrantClient(":memory:")
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return None

def main():
    print("🚀 Запуск примера работы с Qdrant...")
    
    # Ваш документ разбиваем на чанки
    document = """
    Компания 'Ромашка' была основана в 2010 году. 
    Основной продукт - экологичные сумки из переработанных материалов.
    Генеральный директор - Иван Петров.
    Штаб-квартира находится в Москве.
    """

    # Разбиваем на чанки (здесь просто по строчно)
    chunks = [
        "Компания 'Ромашка' основана в 2010 году",
        "Основной продукт - экологичные сумки", 
        "Генеральный директор - Иван Петров",
        "Штаб-квартира находится в Москве"
    ]

    print("📝 Загружаем модель для эмбеддингов...")
    # Загружаем модель
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Создаем векторы
    embeddings = model.encode(chunks)
    print(f"✅ Размерность векторов: {embeddings.shape}")  # (4, 384)

    print("🔗 Подключаемся к Qdrant...")
    # Подключаемся к Qdrant
    client = test_qdrant_connection()
    if client is None:
        print("❌ Не удалось инициализировать Qdrant клиент")
        return

    print("📁 Создаем коллекцию...")
    try:
        # Создаем коллекцию
        client.create_collection(
            collection_name="my_documents",
            vectors_config=VectorParams(size=384, distance=Distance.COSINE)
        )
        print("✅ Коллекция создана")
    except Exception as e:
        print(f"ℹ️ Коллекция уже существует: {e}")

    # Подготавливаем точки для сохранения
    points = [
        PointStruct(
            id=idx,
            vector=embedding.tolist(),
            payload={"text": chunk, "chunk_id": idx}
        )
        for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings))
    ]

    print("💾 Сохраняем векторы в Qdrant...")
    # Сохраняем в Qdrant
    client.upsert(collection_name="my_documents", points=points)
    print("✅ Векторы сохранены в Qdrant!")

    # Небольшая пауза для гарантии сохранения
    time.sleep(1)

    # Поиск по базе
    print("🔍 Выполняем поиск...")
    
    # Ваш вопрос
    question = "Кто руководит компанией?"

    # Создаем вектор для вопроса
    question_embedding = model.encode([question]).tolist()[0]

    # Ищем похожие векторы в Qdrant
    search_result = client.search(
        collection_name="my_documents",
        query_vector=question_embedding,
        limit=2  # топ-2 результата
    )

    # Выводим результаты
    print(f"\n🎯 Результаты поиска для: '{question}'")
    print("=" * 50)
    
    if not search_result:
        print("❌ Ничего не найдено")
    else:
        for i, result in enumerate(search_result, 1):
            print(f"{i}. Текст: {result.payload['text']}")
            print(f"   Схожесть: {result.score:.3f}")
            print(f"   ID: {result.id}")
            print("-" * 40)

    print("\n✅ Пример завершен успешно!")

if __name__ == "__main__":
    main()
