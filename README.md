# BigQuery to PostgreSQL Partition Transfer

A robust Python-based data pipeline for automatically transferring partitioned data from Google BigQuery to PostgreSQL via Google Cloud Storage (GCS).

## 🎯 Overview

This project provides an automated solution for transferring daily partitioned data from BigQuery to PostgreSQL. It includes:

- **🔍 Partition Detection**: Automatically identifies yesterday's partitions in BigQuery
- **📤 Data Export**: Exports partitions as CSV files to GCS processing zone
- **📥 Data Loading**: Loads CSV files from GCS to PostgreSQL
- **🔄 Error Handling**: Moves failed files to unprocess zone for retry
- **🐳 Containerization**: Full Docker support for production deployment

## 📁 Project Structure

```
new_partitions_from_BigQuery_to_PostgreSQL/
├── bq_prtitions_to_pg/
│   └── container/
│       ├── main.py                    # Main orchestration script
│       ├── requirements.txt           # Python dependencies
│       ├── Dockerfile                 # Container definition
│       ├── docker-compose.yml         # Docker Compose setup
│       ├── deploy.sh                  # Linux/Mac deployment script
│       ├── deploy.bat                 # Windows deployment script
│       ├── .dockerignore              # Docker build exclusions
│       ├── README_Docker.md           # Docker documentation
│       ├── credentials/               # Service account files
│       │   ├── bq_service_account.json
│       │   └── gcs_service_account.json
│       ├── config/
│       │   └── config.yaml           # Configuration file
│       ├── scripts/
│       │   ├── export_bq_partitions_to_gcs.py
│       │   └── load_partitions_to_pg.py
│       ├── gcp_clients/
│       │   └── clients.py            # GCP client initialization
│       └── postgresql_conn/
│           └── pg_conn.py            # PostgreSQL connection
└── README.md                         # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker (for containerized deployment)
- Google Cloud Platform account
- PostgreSQL database
- Google Cloud service accounts with appropriate permissions

### 1. Local Development

```bash
# Clone the repository
git clone <repository-url>
cd new_partitions_from_BigQuery_to_PostgreSQL/bq_prtitions_to_pg/container

# Install dependencies
pip install -r requirements.txt

# Configure credentials and settings
# 1. Add service account JSON files to credentials/
# 2. Update config/config.yaml with your settings

# Run the pipeline
python main.py
```

### 2. Docker Deployment

```bash
# Navigate to container directory
cd bq_prtitions_to_pg/container

# Build and run with Docker Compose
docker-compose up --build

# Or use deployment scripts
./deploy.sh          # Linux/Mac
deploy.bat           # Windows
```

## ⚙️ Configuration

### 1. Service Account Setup

Create service account JSON files in `credentials/`:

- **BigQuery Service Account**: `bq_service_account.json`
  - Required permissions: BigQuery Data Viewer, BigQuery Job User
- **GCS Service Account**: `gcs_service_account.json`
  - Required permissions: Storage Object Admin

### 2. Configuration File

Update `config/config.yaml`:

```yaml
# BigQuery Configuration
bigquery:
  project_id: "your-gcp-project-id"
  dataset_id: "your-dataset-id"
  service_account_path: "credentials/bq_service_account.json"

# Google Cloud Storage Configuration
gcs:
  bucket_name: "your-gcs-bucket-name"
  service_account_path: "credentials/gcs_service_account.json"

# PostgreSQL Configuration
postgresql:
  host: "your-postgres-host"
  port: 5432
  database: "your-database-name"
  username: "your-username"
  password: "your-password"
  schema: "public"

# Transfer Configuration
transfer:
  batch_size: 1000
  max_workers: 4
```

## 🔄 Workflow

### Step 1: Partition Detection
- Scans BigQuery dataset for partitioned tables
- Identifies yesterday's partitions using `INFORMATION_SCHEMA.PARTITIONS`
- Validates partition existence and accessibility

### Step 2: Data Export to GCS
- Exports identified partitions as CSV files
- Stores files in GCS `processing_zone/` folder
- Handles large datasets with memory-efficient processing

### Step 3: Data Loading to PostgreSQL
- Downloads CSV files from GCS `processing_zone/`
- Loads data into PostgreSQL tables
- Manages file lifecycle:
  - ✅ **Success**: Delete from `processing_zone/`
  - ❌ **Failure**: Move to `unprocess_zone/`

## 📊 GCS Folder Structure

```
your-gcs-bucket/
├── processing_zone/          # Active files being processed
│   ├── table1_20240114.csv
│   └── table2_20240114.csv
└── unprocess_zone/          # Failed files for retry
    └── failed_table_20240114.csv
```

## 🔧 Features

### ✅ Automation
- Automatic partition detection
- Scheduled execution support
- Error recovery mechanisms

### ✅ Performance
- Minimal logging for efficiency
- Batch processing capabilities
- Memory-optimized data handling

### ✅ Security
- Service account authentication
- Non-root container execution
- Read-only volume mounts

### ✅ Monitoring
- Health checks
- Detailed error reporting
- Success/failure tracking

### ✅ Scalability
- Docker containerization
- Kubernetes ready
- Horizontal scaling support

## 🐳 Docker Deployment

### Build and Run

```bash
# Using Docker Compose (Recommended)
docker-compose up --build

# Using Docker directly
docker build -t bq-to-pg-transfer .
docker run --rm \
  -v $(pwd)/credentials:/app/credentials:ro \
  -v $(pwd)/config:/app/config:ro \
  bq-to-pg-transfer
```

### Volume Mounts

- `./credentials:/app/credentials:ro` - Service account files
- `./config:/app/config:ro` - Configuration files

## 🔄 Scheduling

### Cron Job (Linux/Mac)
```bash
# Add to crontab for daily execution at 2 AM
0 2 * * * cd /path/to/container && ./deploy.sh
```

### Task Scheduler (Windows)
```cmd
# Create scheduled task for daily execution
schtasks /create /tn "BQ-to-PG-Transfer" /tr "C:\path\to\deploy.bat" /sc daily /st 02:00
```

### Docker Cron
```yaml
# Add to docker-compose.yml
services:
  bq-to-pg-transfer:
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

### Health Check
```bash
# Check container health
docker inspect bq-to-pg-transfer | grep Health -A 10
```

## 🐛 Troubleshooting

### Common Issues

1. **Service Account Not Found**
   ```bash
   # Check if files exist
   ls -la credentials/
   ```

2. **Configuration Errors**
   ```bash
   # Validate config file
   python -c "import yaml; yaml.safe_load(open('config/config.yaml'))"
   ```

3. **Permission Denied**
   ```bash
   # Make deployment script executable
   chmod +x deploy.sh
   ```

4. **Network Issues**
   ```bash
   # Check container logs
   docker logs bq-to-pg-transfer
   ```

## 🔒 Security Best Practices

1. **Service Accounts**: Use least privilege principle
2. **Secrets**: Consider using Docker secrets for passwords
3. **Networks**: Use custom networks for isolation
4. **Updates**: Regularly update base image and dependencies

## 📈 Production Deployment

### Environment Variables
```bash
export POSTGRES_PASSWORD="your-secure-password"
export GOOGLE_APPLICATION_CREDENTIALS="/app/credentials/bq_service_account.json"
```

### Kubernetes
```yaml
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

## 📋 Dependencies

### Python Packages
- `google-cloud-bigquery>=3.0.0`
- `google-cloud-storage>=2.0.0`
- `psycopg2-binary>=2.9.0`
- `pandas>=1.5.0`
- `numpy>=1.21.0`
- `PyYAML>=6.0`
- `python-dateutil>=2.8.0`

### System Dependencies
- Python 3.11+
- Docker (for containerized deployment)
- PostgreSQL client libraries

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review the Docker documentation
3. Create an issue in the repository

## 🔄 Version History

- **v1.0.0**: Initial release with basic functionality
- **v1.1.0**: Added Docker support and optimization
- **v1.2.0**: Enhanced error handling and monitoring

---

**Built with ❤️ for automated data pipelines**