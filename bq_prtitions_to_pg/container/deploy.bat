@echo off
REM BigQuery to PostgreSQL Transfer - Docker Deployment Script

echo 🐳 Building Docker image...
docker build -t bq-to-pg-transfer .

if %ERRORLEVEL% EQU 0 (
    echo ✅ Docker image built successfully!
    
    echo 🚀 Starting container...
    docker run --rm ^
        --name bq-to-pg-transfer ^
        -v %cd%/credentials:/app/credentials:ro ^
        -v %cd%/config:/app/config:ro ^
        bq-to-pg-transfer
    
    echo ✅ Container execution completed!
) else (
    echo ❌ Docker build failed!
    exit /b 1
) 