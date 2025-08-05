# Docker Setup for BigQuery to PostgreSQL Transfer

## 🐳 Containerization Files

### Files Created:
- `Dockerfile` - Main container definition
- `.dockerignore` - Excludes unnecessary files from build
- `docker-compose.yml` - Easy deployment with Docker Compose
- `deploy.sh` - Linux/Mac deployment script
- `deploy.bat` - Windows deployment script

## 🚀 Quick Start

### Option 1: Using Docker Compose (Recommended)
```bash
# Build and run with Docker Compose
docker-compose up --build

# Run in background
docker-compose up -d --build
```

### Option 2: Using Docker Directly
```bash
# Build the image
docker build -t bq-to-pg-transfer .

# Run the container
docker run --rm \
  --name bq-to-pg-transfer \
  -v $(pwd)/credentials:/app/credentials:ro \
  -v $(pwd)/config:/app/config:ro \
  bq-to-pg-transfer
```

### Option 3: Using Deployment Scripts
```bash
# Linux/Mac
./deploy.sh

# Windows
deploy.bat
```

## 📁 Volume Mounts

The container mounts these directories:
- `./credentials:/app/credentials:ro` - Service account JSON files
- `./config:/app/config:ro` - Configuration files

## ⚙️ Configuration

### 1. Service Account Files
Add your Google Cloud service account JSON files to `credentials/`:
- `bq_service_account.json` - BigQuery service account
- `gcs_service_account.json` - GCS service account

### 2. Configuration File
Update `config/config.yaml` with your settings:
```yaml
bigquery:
  project_id: "your-project-id"
  dataset_id: "your-dataset-id"

gcs:
  bucket_name: "your-bucket-name"

postgresql:
  host: "your-postgres-host"
  database: "your-database"
  username: "your-username"
  password: "your-password"
```

## 🔧 Docker Features

### Security
- ✅ Non-root user (`app`)
- ✅ Read-only volume mounts
- ✅ Minimal base image (Python 3.11-slim)

### Performance
- ✅ Multi-stage build optimization
- ✅ Layer caching for dependencies
- ✅ Health check included

### Monitoring
- ✅ Health check every 30 seconds
- ✅ Container logs available
- ✅ Exit codes for automation

## 🐛 Troubleshooting

### Common Issues:

1. **Permission Denied**
   ```bash
   # Make deployment script executable
   chmod +x deploy.sh
   ```

2. **Service Account Not Found**
   ```bash
   # Check if files exist
   ls -la credentials/
   ```

3. **Configuration Errors**
   ```bash
   # Validate config file
   python -c "import yaml; yaml.safe_load(open('config/config.yaml'))"
   ```

4. **Network Issues**
   ```bash
   # Check container logs
   docker logs bq-to-pg-transfer
   ```

## 🔄 Scheduling

### Cron Job (Linux/Mac)
```bash
# Add to crontab for daily execution
0 2 * * * cd /path/to/container && ./deploy.sh
```

### Task Scheduler (Windows)
```cmd
# Create scheduled task to run deploy.bat daily
schtasks /create /tn "BQ-to-PG-Transfer" /tr "C:\path\to\deploy.bat" /sc daily /st 02:00
```

### Docker Cron
```yaml
# Add to docker-compose.yml
services:
  bq-to-pg-transfer:
    # ... existing config ...
    command: ["sh", "-c", "while true; do python main.py; sleep 86400; done"]
```

## 📊 Monitoring

### View Logs
```bash
# Docker Compose
docker-compose logs -f

# Docker
docker logs -f bq-to-pg-transfer
```

### Check Health
```bash
# Check container health
docker inspect bq-to-pg-transfer | grep Health -A 10
```

## 🔒 Security Best Practices

1. **Service Accounts**: Use least privilege principle
2. **Secrets**: Consider using Docker secrets for passwords
3. **Networks**: Use custom networks for isolation
4. **Updates**: Regularly update base image and dependencies

## 📈 Production Deployment

### Environment Variables
```bash
# Set environment variables
export POSTGRES_PASSWORD="your-secure-password"
export GOOGLE_APPLICATION_CREDENTIALS="/app/credentials/bq_service_account.json"
```

### Docker Swarm
```bash
# Deploy as a service
docker service create --name bq-transfer bq-to-pg-transfer
```

### Kubernetes
```yaml
# Create deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bq-to-pg-transfer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bq-to-pg-transfer
  template:
    metadata:
      labels:
        app: bq-to-pg-transfer
    spec:
      containers:
      - name: bq-transfer
        image: bq-to-pg-transfer:latest
        volumeMounts:
        - name: credentials
          mountPath: /app/credentials
          readOnly: true
        - name: config
          mountPath: /app/config
          readOnly: true
      volumes:
      - name: credentials
        secret:
          secretName: gcp-credentials
      - name: config
        configMap:
          name: app-config
``` 