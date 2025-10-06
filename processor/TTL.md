```docker
services:
  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
    volumes:
      - qdrant_data:/storage

  log-processor:
    build: .
    command: python3 ttl_processor.py 7
    depends_on:
      - qdrant
    environment:
      - QDRANT_HOST=qdrant
    stdin_open: true
    tty: true

  ttl-cleaner:
    build: .
    command: python3 ttl_daemon.py
    depends_on:
      - qdrant
    environment:
      - QDRANT_HOST=qdrant

volumes:
  qdrant_data:
```
