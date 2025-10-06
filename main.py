from sentence_transformers import SentenceTransformer
import qdrant_client
from qdrant_client.models import PointStruct, VectorParams, Distance



# Ваш документ разбиваем на чанки
document = """
Компания 'Ромашка' была основана в 2010 году. 
Основной продукт - экологичные сумки из переработанных материалов.
Генеральный директор - Иван Петров.
Штаб-квартира находится в Москве.
"""

# Рзбиваем на чанки(здесь просто по строчно)
chunks = [
    "Компания 'Ромашка' основана в 2010 году",
    "Основной продукт - экологичные сумки", 
    "Генеральный директор - Иван Петров",
    "Штаб-квартира находится в Москве"
]

# Загружаем модель
model = SentenceTransformer('all-MiniLM-L6-v2')

# Создаем векторы
embeddings = model.encode(chunks)
print(f"Размерность векторов: {embeddings.shape}")  # (4, 384)

# Подключаемся к Qdrant
client = qdrant_client.QdrantClient(":memory:")  # или "localhost:6333"

# Создаем коллекцию
client.create_collection(
    collection_name="my_documents",
    vectors_config=VectorParams(size=384, distance=Distance.COSINE)
)

# Подготавливаем точки для сохранения
points = [
    PointStruct(
        id=idx,
        vector=embedding.tolist(),
        payload={"text": chunk, "chunk_id": idx}
    )
    for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings))
]

# Сохраняем в Qdrant
client.upsert(collection_name="my_documents", points=points)
print("✅ Векторы сохранены в Qdrant!")

# Поиск по базе
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
print("🔍 Найденные релевантные фрагменты:")
for result in search_result:
    print(f"Текст: {result.payload['text']}")
    print(f"Схожесть: {result.score:.3f}")
    print("---")
