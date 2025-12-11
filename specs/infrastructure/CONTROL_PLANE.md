# Control Configuration Domain Specification

**Version:** 1.0  
**Status:** Specification  
**Durability:** Midge-backed (persistent)  
**Last Updated:** December 11, 2025  

---

## Overview

The Control Configuration domain manages system-wide persistent settings that define Fitz's operational behavior. This includes routing policies, rate limits, quotas, feature flags, and system parameters that survive restarts.

### Key Features

- **System settings**: Global operational parameters
- **Realm quotas**: Per-realm resource limits
- **Feature flags**: Enable/disable features per realm
- **Routing policies**: Static route configuration
- **Rate limits**: Per-realm or per-operation throttling
- **Retention policies**: Data lifecycle rules

### Durability Characteristics

- **Persistent**: All settings stored in Midge
- **Versioned**: Changes tracked with versions
- **Hot reload**: Updates apply without restart
- **Bootstrapped**: Loaded on system startup

### Use Cases

- Realm creation and configuration
- Quota management
- Feature rollout control
- Performance tuning
- Compliance and data retention

---

## Route Format

Control configuration routes:

```
ctrlcfg://system/{setting}/{operation}
ctrlcfg://{realm}/quota/{operation}
ctrlcfg://{realm}/features/{operation}
```

### Examples
- `ctrlcfg://system/settings/get` - Get global settings
- `ctrlcfg://acme/quota/set` - Set realm quotas
- `ctrlcfg://acme/features/enable` - Enable feature flag

---

## Core Operations

### 1. Set System Settings

Configure global system parameters.

**Route:** `ctrlcfg://system/settings/set`

**Request (TLV):**
```
Type: 0x0900 (Control Config Request)
Tags:
  0x04 (operation)    → "set_settings"
  0x10 (settings)     → JSON settings document
```

**Settings Format:**
```json
{
  "max_connections": 10000,
  "max_realms": 100,
  "default_message_ttl_secs": 86400,
  "max_message_size_bytes": 1048576,
  "metrics_flush_interval_secs": 60,
  "log_level": "info"
}
```

**Response:**
```
Type: 0x0901 (Control Config Response)
Tags:
  0x01 (status)       → "ok"
  0x10 (version)      → varint(3)
```

**Storage:**
```
Key: ctrlcfg/system/settings
Value: { ... settings JSON ... }
```

---

### 2. Set Realm Quota

Define resource quotas for a realm.

**Route:** `ctrlcfg://{realm}/quota/set`

**Request:**
```
Type: 0x0900
Tags:
  0x01 (realm)        → "acme"
  0x04 (operation)    → "set_quota"
  0x10 (quota)        → JSON quota document
```

**Quota Format:**
```json
{
  "max_connections": 1000,
  "max_streams": 100,
  "max_queues": 50,
  "max_kv_keys": 10000,
  "max_storage_bytes": 10737418240,
  "rate_limits": {
    "stream_writes_per_sec": 1000,
    "queue_writes_per_sec": 500,
    "kv_ops_per_sec": 5000
  }
}
```

**Response:**
```
Type: 0x0901
Tags:
  0x01 (status)       → "ok"
```

---

### 3. Enable/Disable Feature

Control feature flags per realm.

**Route:** `ctrlcfg://{realm}/features/{feature_name}/enable`

**Request:**
```
Type: 0x0900
Tags:
  0x01 (realm)        → "acme"
  0x04 (operation)    → "enable_feature"
  0x10 (feature)      → "advanced_metrics"
  0x11 (enabled)      → bool(true)
```

**Response:**
```
Type: 0x0901
Tags:
  0x01 (status)       → "ok"
```

**Storage:**
```
Key: ctrlcfg/{realm}/features/{feature_name}
Value: { "enabled": true, "updated_at": "..." }
```

---

### 4. Set Retention Policy

Configure data retention rules.

**Route:** `ctrlcfg://{realm}/retention/set`

**Request:**
```
Type: 0x0900
Tags:
  0x01 (realm)        → "acme"
  0x04 (operation)    → "set_retention"
  0x10 (policy)       → JSON policy document
```

**Policy Format:**
```json
{
  "streams": {
    "default_ttl_days": 30,
    "max_ttl_days": 365,
    "patterns": [
      {"path": "stream://acme/audit/*", "ttl_days": 2555}
    ]
  },
  "queues": {
    "default_ttl_days": 7,
    "message_retention_count": 10000
  },
  "kv": {
    "default_ttl_days": null,
    "patterns": [
      {"path": "kv://acme/cache/*", "ttl_days": 1}
    ]
  }
}
```

---

### 5. Create Realm

Initialize a new realm with default configuration.

**Route:** `ctrlcfg://system/realms/create`

**Request:**
```
Type: 0x0900
Tags:
  0x01 (realm)        → "newco"
  0x04 (operation)    → "create_realm"
  0x10 (config)       → JSON realm config
```

**Realm Config:**
```json
{
  "name": "newco",
  "display_name": "NewCo Inc.",
  "quota": { ... },
  "features": ["streams", "queues", "kv", "metrics"],
  "retention": { ... },
  "auth_required": true
}
```

**Response:**
```
Type: 0x0901
Tags:
  0x01 (status)       → "ok"
  0x10 (realm_id)     → "newco"
```

**Storage:**
```
Key: ctrlcfg/realms/{realm_name}
Value: { ... realm config ... }
```

---

## Actor Implementation

### ControlConfigActor State

```rust
pub struct ControlConfigActor {
    /// Storage bridge
    midge: ActorRef<MidgeMsg>,
    
    /// In-memory cache
    system_settings: Arc<RwLock<SystemSettings>>,
    realm_quotas: Arc<DashMap<String, RealmQuota>>,
    feature_flags: Arc<DashMap<String, FeatureFlags>>,
    retention_policies: Arc<DashMap<String, RetentionPolicy>>,
    realms: Arc<DashMap<String, RealmConfig>>,
    
    /// Actors to notify on config changes
    control_runtime: ActorRef<ControlRuntimeMsg>,
    realm_actor: ActorRef<RealmMsg>,
}

#[derive(Debug, Clone)]
struct SystemSettings {
    max_connections: usize,
    max_realms: usize,
    default_message_ttl_secs: u64,
    max_message_size_bytes: usize,
    metrics_flush_interval_secs: u64,
    log_level: String,
    version: u64,
}

#[derive(Debug, Clone)]
struct RealmQuota {
    max_connections: usize,
    max_streams: usize,
    max_queues: usize,
    max_kv_keys: usize,
    max_storage_bytes: u64,
    rate_limits: RateLimits,
}

#[derive(Debug, Clone)]
struct RateLimits {
    stream_writes_per_sec: u64,
    queue_writes_per_sec: u64,
    kv_ops_per_sec: u64,
}

#[derive(Debug, Clone)]
struct FeatureFlags {
    realm: String,
    enabled: HashMap<String, bool>,
}

#[derive(Debug, Clone)]
struct RetentionPolicy {
    streams: Option<DomainRetention>,
    queues: Option<DomainRetention>,
    kv: Option<DomainRetention>,
}

#[derive(Debug, Clone)]
struct RealmConfig {
    name: String,
    display_name: String,
    quota: RealmQuota,
    features: Vec<String>,
    retention: RetentionPolicy,
    auth_required: bool,
    created_at: Instant,
}
```

---

### Message Handler

```rust
impl Actor for ControlConfigActor {
    type Message = ControlConfigMsg;
    
    fn on_message(&mut self, msg: Self::Message, ctx: &ActorContext<Self>) {
        match msg {
            ControlConfigMsg::SetSystemSettings { settings, reply_to } => {
                // Persist to Midge
                let key = "ctrlcfg/system/settings".to_string();
                let value = serde_json::to_vec(&settings).unwrap();
                
                self.midge.send(MidgeMsg::KvPut {
                    realm: "_system".to_string(),
                    area: "control".to_string(),
                    key,
                    value,
                    ttl: None,
                    reply_to: ActorRef::dead(),
                });
                
                // Update cache
                *self.system_settings.write().unwrap() = settings.clone();
                
                // Notify control runtime
                self.control_runtime.send(ControlRuntimeMsg::SettingsUpdated {
                    settings: settings.clone(),
                });
                
                reply_to.send(ControlConfigReply::Ok { version: settings.version });
            }
            
            ControlConfigMsg::SetRealmQuota { realm, quota, reply_to } => {
                // Persist to Midge
                let key = format!("ctrlcfg/{}/quota", realm);
                let value = serde_json::to_vec(&quota).unwrap();
                
                self.midge.send(MidgeMsg::KvPut {
                    realm: realm.clone(),
                    area: "_system".to_string(),
                    key,
                    value,
                    ttl: None,
                    reply_to: ActorRef::dead(),
                });
                
                // Update cache
                self.realm_quotas.insert(realm.clone(), quota.clone());
                
                // Notify realm actor
                self.realm_actor.send(RealmMsg::QuotaUpdated {
                    realm: realm.clone(),
                    quota,
                });
                
                reply_to.send(ControlConfigReply::Ok { version: 1 });
            }
            
            ControlConfigMsg::SetFeatureFlag { realm, feature, enabled, reply_to } => {
                // Update feature flags
                let mut flags = self.feature_flags
                    .entry(realm.clone())
                    .or_insert_with(|| FeatureFlags {
                        realm: realm.clone(),
                        enabled: HashMap::new(),
                    });
                flags.enabled.insert(feature.clone(), enabled);
                
                // Persist to Midge
                let key = format!("ctrlcfg/{}/features/{}", realm, feature);
                let value = serde_json::to_vec(&enabled).unwrap();
                
                self.midge.send(MidgeMsg::KvPut {
                    realm: realm.clone(),
                    area: "_system".to_string(),
                    key,
                    value,
                    ttl: None,
                    reply_to: ActorRef::dead(),
                });
                
                reply_to.send(ControlConfigReply::Ok { version: 1 });
            }
            
            ControlConfigMsg::CreateRealm { config, reply_to } => {
                // Store realm config
                let key = format!("ctrlcfg/realms/{}", config.name);
                let value = serde_json::to_vec(&config).unwrap();
                
                self.midge.send(MidgeMsg::KvPut {
                    realm: "_system".to_string(),
                    area: "control".to_string(),
                    key,
                    value,
                    ttl: None,
                    reply_to: ActorRef::dead(),
                });
                
                // Update cache
                self.realms.insert(config.name.clone(), config.clone());
                
                // Notify realm actor to initialize
                self.realm_actor.send(RealmMsg::Initialize {
                    realm: config.name.clone(),
                    config: config.clone(),
                });
                
                reply_to.send(ControlConfigReply::RealmCreated {
                    realm_id: config.name,
                });
            }
        }
    }
}
```

---

## Bootstrap Process

On system startup:

```rust
impl ControlConfigActor {
    pub fn load_bootstrap_config(&mut self) {
        // Load system settings
        self.midge.send(MidgeMsg::KvGet {
            realm: "_system".to_string(),
            area: "control".to_string(),
            key: "ctrlcfg/system/settings".to_string(),
            reply_to: self_ref.clone(),
        });
        
        // Load all realm configs
        self.midge.send(MidgeMsg::KvScan {
            realm: "_system".to_string(),
            area: "control".to_string(),
            prefix: "ctrlcfg/realms/".to_string(),
            reply_to: self_ref.clone(),
        });
        
        // Notify control runtime when loaded
    }
}
```

---

## Hot Reload

Configuration changes apply immediately:

1. Control plane sends update to ControlConfigActor
2. ControlConfigActor persists to Midge
3. ControlConfigActor updates in-memory cache
4. ControlConfigActor notifies relevant actors:
   - ControlRuntimeMsg for system settings
   - RealmMsg for realm quotas
   - SessionMsg for connection limits

---

## Error Handling

### Error Codes

- `INVALID_SETTINGS` - Malformed configuration
- `REALM_EXISTS` - Realm already created
- `REALM_NOT_FOUND` - Realm doesn't exist
- `QUOTA_EXCEEDED` - Cannot create realm (global limit)
- `STORAGE_ERROR` - Midge write failure

### Validation

- Settings must pass schema validation
- Quotas must be non-negative
- Feature names must be recognized
- Realm names must be valid identifiers

---

## Performance Characteristics

### Latency

- **Config write**: <10ms (Midge write)
- **Config read (cached)**: <100µs
- **Config read (cold)**: <5ms (Midge read)
- **Hot reload**: <1ms (in-memory update)

### Caching

- All config cached in-memory
- Write-through cache invalidation
- Lazy load on demand

---

## Testing Strategy

### Unit Tests

- Settings validation
- Quota enforcement
- Feature flag toggling
- Realm creation

### Integration Tests

- End-to-end config updates
- Hot reload behavior
- Bootstrap loading
- Cache consistency

---

## References

- [Control Runtime Domain](../ephemeral/CONTROL_RUNTIME.md)
- [Realm Domain](../ephemeral/REALMS.md)
- [Midge Storage](MIDGE.md)
