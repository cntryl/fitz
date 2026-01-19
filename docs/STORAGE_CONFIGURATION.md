# Fitz Storage Configuration

## Overview

The Fitz broker supports three storage backends, selected via environment variables:

1. **In-Memory** (ephemeral, no persistence)
2. **Local Disk** (durable, file-backed) [DEFAULT]
3. **Cloud** (S3, GCS, Azure)

## Quick Reference

### Environment Variables

```bash
# Primary: Choose storage mode
FITZ_STORAGE_MODE=memory|local|s3|gcs|azure

# For local disk storage
FITZ_STORAGE_PATH=/path/to/db              # Default: ./.fitz

# For cloud storage
FITZ_STORAGE_PROVIDER=s3|gcs|azure
FITZ_STORAGE_BUCKET=bucket-name
FITZ_STORAGE_PREFIX=optional/prefix         # Optional

# Cloud credentials (required for respective provider)
AWS_ACCESS_KEY_ID=xxx          # For S3
AWS_SECRET_ACCESS_KEY=xxx
AWS_REGION=us-east-1

GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceaccount.json  # For GCS

AZURE_STORAGE_ACCOUNT_NAME=xxx              # For Azure
AZURE_STORAGE_ACCOUNT_KEY=xxx
```

## Storage Modes

### 1. In-Memory Storage

**Use case:** Testing, development, stateless deployments

**Configuration:**
```bash
export FITZ_STORAGE_MODE=memory
cargo run --release
```

**Characteristics:**
- ❌ No persistence - data lost on shutdown
- ✅ Fast startup (no disk I/O)
- ✅ Perfect for unit/integration tests
- ✅ Zero disk space requirement
- ❌ Not suitable for production

**Startup Log:**
```
INFO fitz::boot::storage: Initializing in-memory storage (ephemeral, no persistence)
INFO fitz::boot::storage: In-memory storage ready (data lost on shutdown)
```

### 2. Local Disk Storage

**Use case:** Single-node deployments, development, testing durability

**Configuration:**
```bash
# Default (uses ./.fitz directory)
cargo run --release

# Or explicit path
export FITZ_STORAGE_PATH=/var/lib/fitz/data
cargo run --release

# Or via config
FITZ_STORAGE_MODE=local
FITZ_STORAGE_PATH=/mnt/fitz-data
```

**Characteristics:**
- ✅ Durable, persistent storage
- ✅ Full ACID semantics via Midge LSM
- ✅ Automatic recovery on restart
- ✅ Backup via file copy
- ✅ Single-node deployments
- ⚠️ Not suitable for distributed setups

**Startup Log:**
```
INFO fitz::boot::storage: Initializing local disk storage at /var/lib/fitz/data
INFO fitz::boot::storage: Local disk storage ready at /var/lib/fitz/data
```

**Data Directory Structure:**
```
./.fitz/
├── manifest.json          # Database metadata
├── manifest.journal       # Manifest changes log
├── wal/                   # Write-ahead logs
├── levels/                # LSM tree levels
└── snapshots/             # Periodic snapshots
```

### 3. Cloud Storage (S3, GCS, Azure)

**Use case:** Distributed deployments, scalability, high availability

#### S3 (Amazon)

**Configuration:**
```bash
export FITZ_STORAGE_MODE=s3
export FITZ_STORAGE_BUCKET=my-fitz-data
export FITZ_STORAGE_PREFIX=prod/us-east-1    # Optional

# AWS credentials (one of):
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-east-1

# OR use IAM role (EC2, ECS, etc) - no explicit credentials needed
```

**Startup Log:**
```
INFO fitz::boot::storage: Initializing cloud storage: provider=s3 bucket=my-fitz-data prefix=Some("prod")
INFO fitz::boot::storage: S3 credentials detected
INFO fitz::boot::storage: Cloud storage ready: s3 bucket=my-fitz-data prefix=Some("prod")
```

#### GCS (Google Cloud)

**Configuration:**
```bash
export FITZ_STORAGE_MODE=gcs
export FITZ_STORAGE_BUCKET=my-fitz-data
export FITZ_STORAGE_PREFIX=prod

# Service account credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

**Startup Log:**
```
INFO fitz::boot::storage: Initializing cloud storage: provider=gcs bucket=my-fitz-data
INFO fitz::boot::storage: GCS credentials detected
INFO fitz::boot::storage: Cloud storage ready: gcs bucket=my-fitz-data
```

#### Azure Storage

**Configuration:**
```bash
export FITZ_STORAGE_MODE=azure
export FITZ_STORAGE_BUCKET=mycontainer    # Container name
export FITZ_STORAGE_PREFIX=prod

export AZURE_STORAGE_ACCOUNT_NAME=myaccount
export AZURE_STORAGE_ACCOUNT_KEY=key...
```

**Startup Log:**
```
INFO fitz::boot::storage: Initializing cloud storage: provider=azure bucket=mycontainer
INFO fitz::boot::storage: Azure credentials detected
INFO fitz::boot::storage: Cloud storage ready: azure bucket=mycontainer prefix=Some("prod")
```

## Configuration Detection

The storage mode is detected in this priority order:

1. **Environment Variable `FITZ_STORAGE_MODE`**
   - If set, use specified mode (memory, local, s3, gcs, azure)
   - Case-insensitive

2. **Mode-Specific Variables**
   - Local: `FITZ_STORAGE_PATH` (default: ./.fitz)
   - Cloud: `FITZ_STORAGE_PROVIDER`, `FITZ_STORAGE_BUCKET`, `FITZ_STORAGE_PREFIX`

3. **Cloud Credentials**
   - S3: AWS_ACCESS_KEY_ID or AWS_PROFILE
   - GCS: GOOGLE_APPLICATION_CREDENTIALS
   - Azure: AZURE_STORAGE_ACCOUNT_NAME

4. **Default**
   - If nothing specified: Local Disk at `./.fitz`

## Code Examples

### Programmatic Configuration

```rust
use fitz::boot::{BootConfig, StorageMode};

// In-memory storage
let config = BootConfig::with_memory_storage();

// Local disk storage
let config = BootConfig::with_local_storage("/var/lib/fitz");

// Cloud storage (S3)
let config = BootConfig::default().with_storage_mode(
    StorageMode::CloudBacked {
        provider: "s3".to_string(),
        bucket: "my-bucket".to_string(),
        prefix: Some("prod".to_string()),
    },
);

// Start broker
fitz::boot::boot(config).await?;
```

### Testing

```rust
#[tokio::test]
async fn should_work_with_memory_storage() {
    // Arrange - Use in-memory storage for testing
    let config = BootConfig::with_memory_storage();
    
    // Act
    let result = fitz::boot::boot(config).await;
    
    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_work_with_local_storage() {
    // Arrange - Use temporary directory
    let temp_dir = tempfile::tempdir().unwrap();
    let config = BootConfig::with_local_storage(
        temp_dir.path().to_str().unwrap()
    );
    
    // Act
    let result = fitz::boot::boot(config).await;
    
    // Assert
    assert!(result.is_ok());
}
```

## Startup Logs

### In-Memory
```
INFO fitz::boot: Starting Fitz broker
INFO fitz::boot::storage: Initializing in-memory storage (ephemeral, no persistence)
INFO fitz::boot::storage: In-memory storage ready (data lost on shutdown)
INFO fitz::boot: Storage initialized
```

### Local Disk
```
INFO fitz::boot: Starting Fitz broker
INFO fitz::boot::storage: Initializing local disk storage at ./.fitz
INFO fitz::boot::storage: Local disk storage ready at ./.fitz
INFO fitz::boot: Storage initialized
```

### S3
```
INFO fitz::boot: Starting Fitz broker
INFO fitz::boot::storage: Initializing cloud storage: provider=s3 bucket=my-bucket prefix=Some("prod")
INFO fitz::boot::storage: S3 credentials detected
INFO fitz::boot::storage: Cloud storage ready: s3 bucket=my-bucket prefix=Some("prod")
INFO fitz::boot: Storage initialized
```

## Environment Variable Examples

### Development (Default Local)
```bash
# No env vars needed - defaults to ./.fitz
cargo run
```

### Testing (In-Memory)
```bash
FITZ_STORAGE_MODE=memory cargo test
```

### Staging (Local Custom Path)
```bash
FITZ_STORAGE_MODE=local
FITZ_STORAGE_PATH=/mnt/staging/fitz
cargo run --release
```

### Production (S3)
```bash
FITZ_STORAGE_MODE=s3
FITZ_STORAGE_BUCKET=fitz-prod-us-east-1
FITZ_STORAGE_PREFIX=v1
AWS_REGION=us-east-1
# AWS credentials via IAM role (on EC2/ECS)
cargo run --release
```

### Production (GCS)
```bash
FITZ_STORAGE_MODE=gcs
FITZ_STORAGE_BUCKET=fitz-prod
FITZ_STORAGE_PREFIX=us-central1
GOOGLE_APPLICATION_CREDENTIALS=/var/secrets/gcs-key.json
cargo run --release
```

### Docker Compose Example

```yaml
services:
  fitz:
    image: fitz:latest
    environment:
      FITZ_STORAGE_MODE: local
      FITZ_STORAGE_PATH: /data/fitz
      RUST_LOG: fitz=info
    volumes:
      - fitz-data:/data/fitz
    ports:
      - "4090:4090"  # HTTP/WebSocket
      - "4091:4091"  # TCP
    
volumes:
  fitz-data:
    driver: local
```

### Kubernetes Example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fitz-config
data:
  FITZ_STORAGE_MODE: "s3"
  FITZ_STORAGE_BUCKET: "fitz-prod"
  FITZ_STORAGE_PREFIX: "k8s/us-west-2"
  RUST_LOG: "fitz=info"

---
apiVersion: v1
kind: Secret
metadata:
  name: fitz-aws
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "AKIA..."
  AWS_SECRET_ACCESS_KEY: "wJal..."
  AWS_REGION: "us-west-2"

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fitz
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fitz
  template:
    metadata:
      labels:
        app: fitz
    spec:
      containers:
      - name: fitz
        image: fitz:latest
        envFrom:
        - configMapRef:
            name: fitz-config
        - secretRef:
            name: fitz-aws
        ports:
        - containerPort: 4090
          name: http
        - containerPort: 4091
          name: tcp
```

## Migration Between Backends

### Local → S3

```bash
# 1. Export data from local storage
# (Midge provides export utilities)

# 2. Migrate data to S3
# (Use AWS S3 CLI or Midge migration tools)

# 3. Update environment
export FITZ_STORAGE_MODE=s3
export FITZ_STORAGE_BUCKET=my-bucket
export AWS_REGION=us-east-1

# 4. Start broker
cargo run --release
```

## Performance Characteristics

| Metric | In-Memory | Local Disk | S3 | GCS | Azure |
|--------|-----------|-----------|----|----|-------|
| Latency | <1ms | 1-10ms | 10-100ms | 10-100ms | 10-100ms |
| Throughput | Very High | High | Medium | Medium | Medium |
| Durability | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| Scalability | Single Node | Single Node | Global | Global | Global |
| Cost | Free | Disk space | $0.023/GB/mo | $0.020/GB/mo | $0.0124/GB/mo |
| Setup Time | Instant | Seconds | Minutes | Minutes | Minutes |

## Troubleshooting

### ERROR: Failed to open in-memory Midge
```
Cause: Midge LSM engine not properly initialized
Solution: Ensure Midge is properly compiled and linked
```

### ERROR: Failed to create storage directory
```
Cause: No write permission to directory
Solution: chmod +w ./.fitz or use different path with write access
```

### ERROR: AWS_ACCESS_KEY_ID required for S3 storage
```
Cause: Missing AWS credentials
Solution: Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or use IAM role
```

### ERROR: GOOGLE_APPLICATION_CREDENTIALS required for GCS storage
```
Cause: Missing GCS credentials
Solution: Set GOOGLE_APPLICATION_CREDENTIALS to valid service account JSON
```

## Testing

Run tests with different storage modes:

```bash
# All tests with default (local) storage
cargo test --lib

# All tests with in-memory storage
FITZ_STORAGE_MODE=memory cargo test --lib

# Specific storage tests
cargo test --lib boot::storage
```

All storage tests pass:
```
test boot::storage::tests::should_create_boot_config_for_test_storage ... ok
test boot::storage::tests::should_detect_local_storage_by_default ... ok
test boot::storage::tests::should_support_memory_storage_mode ... ok
test boot::storage::tests::should_support_local_storage_mode ... ok
test boot::storage::tests::should_support_cloud_storage_mode ... ok

test result: ok. 5 passed; 0 failed
```

## Future Enhancements

- [ ] Support for additional cloud providers (MinIO, Backblaze, etc)
- [ ] Multi-cloud replication
- [ ] Automatic backup to secondary storage
- [ ] Storage migration utilities
- [ ] Real-time storage metrics
- [ ] Encryption-at-rest for all backends
