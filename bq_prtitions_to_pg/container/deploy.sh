#!/bin/bash

# BigQuery to PostgreSQL Transfer - Docker Deployment Script

echo "🐳 Building Docker image..."
docker build -t bq-to-pg-transfer .

if [ $? -eq 0 ]; then
    echo "✅ Docker image built successfully!"
    
    echo "🚀 Starting container..."
    docker run --rm \
        --name bq-to-pg-transfer \
        -v $(pwd)/credentials:/app/credentials:ro \
        -v $(pwd)/config:/app/config:ro \
        bq-to-pg-transfer
    
    echo "✅ Container execution completed!"
else
    echo "❌ Docker build failed!"
    exit 1
fi 