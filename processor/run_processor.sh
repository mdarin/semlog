#!/bin/bash
# run_processor.sh - Запуск с мониторингом и перезапуском

PROCESSOR_SCRIPT="universal_processor.py"
COLLECTION_NAME="${1:-universal-logs}"
MAX_RESTARTS=5
RESTART_DELAY=5

restart_count=0

while [ $restart_count -lt $MAX_RESTARTS ]; do
    echo "🚀 Starting $PROCESSOR_SCRIPT for $COLLECTION_NAME..."
    
    # Запускаем процессор
    python3 $PROCESSOR_SCRIPT $COLLECTION_NAME
    
    exit_code=$?
    restart_count=$((restart_count + 1))
    
    if [ $exit_code -eq 0 ]; then
        echo "✅ Processor finished normally"
        break
    else
        echo "💥 Processor crashed with exit code $exit_code"
        echo "🔄 Restarting in $RESTART_DELAY seconds... ($restart_count/$MAX_RESTARTS)"
        sleep $RESTART_DELAY
    fi
done

if [ $restart_count -eq $MAX_RESTARTS ]; then
    echo "❌ Max restarts reached. Giving up."
    exit 1
fi
