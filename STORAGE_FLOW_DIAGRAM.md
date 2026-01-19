# Storage Configuration Flow Diagram

## Environment Detection

```
┌─────────────────────────────────────────────────────────────┐
│          FITZ Storage Bootstrap                              │
│     Detecting Storage Backend from Environment               │
└─────────────────────────────────────────────────────────────┘

Step 1: Check FITZ_STORAGE_MODE
         │
         ├─── "memory"          → IN-MEMORY STORAGE
         │       (ephemeral, no persistence)
         │       Suitable for: testing, development
         │       Speed: <1ms, throughput: very high
         │
         ├─── "local"           → LOCAL DISK STORAGE
         │       (file-backed, persistent)
         │       Path: FITZ_STORAGE_PATH (default: ./.fitz)
         │       Suitable for: single-node, local dev
         │       Speed: 1-10ms, throughput: high
         │
         └─── "s3"|"gcs"|"azure" → CLOUD STORAGE
             (distributed, scalable)
             Provider: FITZ_STORAGE_PROVIDER
             Bucket: FITZ_STORAGE_BUCKET
             Prefix: FITZ_STORAGE_PREFIX (optional)
             Suitable for: distributed, production
             Speed: 10-100ms, throughput: medium

Step 2: Validate Credentials
         │
         ├─ S3:    Requires AWS_ACCESS_KEY_ID OR AWS_PROFILE
         ├─ GCS:   Requires GOOGLE_APPLICATION_CREDENTIALS
         └─ Azure: Requires AZURE_STORAGE_ACCOUNT_NAME

Step 3: Initialize Storage Engine
         │
         ├─ Memory:     Create in-memory Midge engine
         ├─ Local:      Create local disk Midge engine
         └─ Cloud:      Create cloud-backed Midge engine

Step 4: Log Configuration
         └─ INFO: Storage initialized with selected backend

═════════════════════════════════════════════════════════════════

DECISION TREE

Start
  │
  ├─ FITZ_STORAGE_MODE set?
  │   │
  │   ├─ YES → memory?
  │   │         ├─ YES → IN-MEMORY (ephemeral)
  │   │         ├─ NO → local?
  │   │         │       ├─ YES → LOCAL DISK at FITZ_STORAGE_PATH
  │   │         │       ├─ NO → cloud?
  │   │         │               ├─ YES (s3|gcs|azure)
  │   │         │               │   └─ Validate credentials
  │   │         │               │   └─ CLOUD STORAGE
  │   │         │               └─ NO → Default to LOCAL DISK
  │   │
  │   └─ NO → Use LOCAL DISK (./.fitz)
  │
  └─ Broker Running ✅

═════════════════════════════════════════════════════════════════

ENVIRONMENT VARIABLE MATRIX

╔═══════════════════════════════════════════════════════════════╗
║ Backend         │ Primary Var      │ Required Additional      ║
╠═══════════════════════════════════════════════════════════════╣
║ Memory          │ =memory          │ None                     ║
║ Local Disk      │ =local OR unset  │ FITZ_STORAGE_PATH (opt)  ║
║ S3              │ =s3              │ FITZ_STORAGE_BUCKET      ║
║                 │                  │ AWS credentials          ║
║ GCS             │ =gcs             │ FITZ_STORAGE_BUCKET      ║
║                 │                  │ GOOGLE_CREDENTIALS       ║
║ Azure           │ =azure           │ FITZ_STORAGE_BUCKET      ║
║                 │                  │ AZURE credentials        ║
╚═══════════════════════════════════════════════════════════════╝

═════════════════════════════════════════════════════════════════

STARTUP SEQUENCE

┌─────────────────────────────────────────────────────────────┐
│ main() → BootConfig::new()                                  │
│           │                                                 │
│           ├─ Check FITZ_STORAGE_MODE env var              │
│           └─ Call StorageMode::from_env()                 │
│                    │                                       │
│                    ├─ memory?     → StorageMode::Memory    │
│                    ├─ local?      → StorageMode::LocalDisk │
│                    └─ cloud?      → StorageMode::CloudBacked
│                                                            │
│           boot(config) → storage::init(&config)           │
│                           │                               │
│                           ├─ Match on config.storage_mode│
│                           │   ├─ Memory    → init_memory()
│                           │   ├─ Local     → init_local()
│                           │   └─ Cloud     → init_cloud() │
│                           │                               │
│                           └─ Arc<MidgeEngine>            │
│                                                            │
│           Runtime initialized ✅                          │
└─────────────────────────────────────────────────────────────┘

═════════════════════════════════════════════════════════════════

DEPLOYMENT EXAMPLES

┌─ Development ──────────────────────────────────────┐
│ $ cargo run                                         │
│                                                    │
│ Uses: LOCAL DISK at ./.fitz (default)             │
│ Data: Persisted between runs                       │
│ Logging: "Initializing local disk storage..."     │
└────────────────────────────────────────────────────┘

┌─ Testing ──────────────────────────────────────────┐
│ $ FITZ_STORAGE_MODE=memory cargo test --lib       │
│                                                    │
│ Uses: IN-MEMORY (ephemeral)                       │
│ Data: Lost after tests complete                   │
│ Logging: "Initializing in-memory storage..."      │
└────────────────────────────────────────────────────┘

┌─ Production (AWS S3) ──────────────────────────────┐
│ $ export FITZ_STORAGE_MODE=s3                     │
│   export FITZ_STORAGE_BUCKET=fitz-prod-us-east-1 │
│   export FITZ_STORAGE_PREFIX=v1                   │
│   export AWS_REGION=us-east-1                     │
│   cargo run --release                              │
│                                                    │
│ Uses: AWS S3 (distributed, scalable)              │
│ Data: Persisted in S3 bucket                      │
│ Logging: "Initializing cloud storage: s3..."      │
│ Credentials: IAM role (on EC2/ECS) or env vars    │
└────────────────────────────────────────────────────┘

┌─ Production (Google GCS) ──────────────────────────┐
│ $ export FITZ_STORAGE_MODE=gcs                    │
│   export FITZ_STORAGE_BUCKET=fitz-prod            │
│   export GOOGLE_APPLICATION_CREDENTIALS=/secret.json
│   cargo run --release                              │
│                                                    │
│ Uses: Google Cloud Storage (distributed, scalable)│
│ Data: Persisted in GCS bucket                     │
│ Logging: "Initializing cloud storage: gcs..."     │
│ Credentials: Service account JSON file            │
└────────────────────────────────────────────────────┘

═════════════════════════════════════════════════════════════════

INITIALIZATION FUNCTIONS

src/boot/storage.rs

┌─ pub async fn init(config: &BootConfig)
│                      ↓
│        Match on config.storage_mode
│        │
│        ├─ Memory → init_memory(config)
│        │           └─ Create ephemeral engine
│        │           └─ Log: "in-memory storage ready"
│        │
│        ├─ LocalDisk { db_path } → init_local_disk(config, db_path)
│        │                           └─ Create directory
│        │                           └─ Create durable engine
│        │                           └─ Log: "local disk ready"
│        │
│        └─ CloudBacked { provider, bucket, prefix } → init_cloud(...)
│                                                       └─ Validate creds
│                                                       └─ Check provider
│                                                       └─ Create cloud engine
│                                                       └─ Log: "cloud ready"
│
└─ Returns: Arc<MidgeEngine>

═════════════════════════════════════════════════════════════════

BOOT CONFIG BUILDERS

BootConfig::new()                           Default (local ./.fitz)
  ↓
BootConfig::with_memory_storage()           In-memory for testing
  ↓
BootConfig::with_local_storage("/path")     Local disk at path
  ↓
BootConfig::default()
  .with_storage_mode(StorageMode::CloudBacked { ... })
                                            Cloud for production

═════════════════════════════════════════════════════════════════

CREDENTIAL VALIDATION

S3:
  ├─ Check: AWS_ACCESS_KEY_ID exists?
  │         OR AWS_PROFILE exists?
  └─ Log: "S3 credentials detected"

GCS:
  ├─ Check: GOOGLE_APPLICATION_CREDENTIALS exists?
  └─ Log: "GCS credentials detected"

Azure:
  ├─ Check: AZURE_STORAGE_ACCOUNT_NAME exists?
  └─ Log: "Azure credentials detected"

═════════════════════════════════════════════════════════════════

SUMMARY: THREE BACKENDS AT YOUR FINGERTIPS

  In-Memory       Local Disk         Cloud (S3/GCS/Azure)
  ─────────       ──────────         ───────────────────
  
  TESTING         DEVELOPMENT        PRODUCTION
  
  ✅ Fast         ✅ Persistent      ✅ Scalable
  ✅ Simple       ✅ Simple          ✅ Reliable
  ❌ No Backup    ⚠️ Single Node     ✅ Distributed
  
  export          cargo run          export FITZ_STORAGE_MODE=s3
  FITZ_STORAGE_   (default)          export FITZ_STORAGE_BUCKET=...
  MODE=memory                         cargo run --release
  cargo test

═════════════════════════════════════════════════════════════════
```

## Answer to "Are we using..."

```
┌─────────────────────────────────────────────────────┐
│  FITZ Storage Configuration Status                   │
├─────────────────────────────────────────────────────┤
│                                                     │
│ Q: Are we using in-memory only database?           │
│ A: ✅ YES (when FITZ_STORAGE_MODE=memory)           │
│                                                     │
│ Q: Are we using local database?                    │
│ A: ✅ YES (when FITZ_STORAGE_MODE=local or default)│
│                                                     │
│ Q: Are we using cloud?                             │
│ A: ✅ YES (when FITZ_STORAGE_MODE=s3|gcs|azure)    │
│                                                     │
│ Q: If cloud, which one?                            │
│ A: ✅ All three!                                    │
│    • S3 (Amazon)      - FITZ_STORAGE_MODE=s3       │
│    • GCS (Google)     - FITZ_STORAGE_MODE=gcs      │
│    • Azure (Microsoft)- FITZ_STORAGE_MODE=azure    │
│                                                     │
│ Environment: Runtime selectable, NO code changes   │
│ Default: Local disk at ./.fitz                     │
│ Tests: Use in-memory for speed                     │
│ Prod: Use cloud for scalability                    │
│                                                     │
└─────────────────────────────────────────────────────┘
```
