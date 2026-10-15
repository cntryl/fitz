//! External resource-capped broker lifecycle and disposable S3 evidence helpers.
//! Only compiled for the opt-in recovery driver; not a production API.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn command(program: &str, args: &[&str]) -> Vec<u8> {
    let result = Command::new(program)
        .args(args)
        .output()
        .expect("qualification command");
    assert!(
        result.status.success(),
        "{program} {args:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    result.stdout
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub serialized_catalog_bytes: u64,
    pub catalog_bytes: u64,
    pub remote_wal_bytes: u64,
    pub remote_wal_objects: usize,
    pub sst_objects: usize,
    pub segment_ids: Vec<u64>,
}

pub struct Campaign {
    pub id: String,
    pub directory: PathBuf,
    bucket: String,
    prefix: String,
    endpoint: String,
}

impl Campaign {
    #[cfg(feature = "recovery-qualification")]
    #[must_use]
    pub fn location(&self) -> cntryl_midge::CloudStorageLocation {
        cntryl_midge::CloudStorageLocation::new(
            cntryl_midge::CloudProviderConfig::s3_compatible_static(
                &self.bucket,
                &self.endpoint,
                "admin",
                "easy-peasy",
            ),
            self.prefix.clone(),
        )
    }

    #[cfg(feature = "recovery-qualification")]
    /// Read and retain an object from the disposable provider.
    ///
    /// # Panics
    /// Panics if the CLI request, temporary file, or local read fails.
    #[must_use]
    pub fn download_object(&self, key: &str) -> Vec<u8> {
        let file = tempfile::NamedTempFile::new().unwrap();
        self.aws(&[
            "s3api",
            "get-object",
            "--bucket",
            &self.bucket,
            "--key",
            &format!("{}{key}", self.prefix),
            file.path().to_str().unwrap(),
        ]);
        std::fs::read(file.path()).unwrap()
    }

    #[cfg(feature = "recovery-qualification")]
    /// Publish an offline fixture object to the disposable provider.
    ///
    /// # Panics
    /// Panics if the temporary file or CLI upload fails.
    pub fn upload_object(&self, key: &str, bytes: &[u8]) {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), bytes).unwrap();
        self.aws(&[
            "s3api",
            "put-object",
            "--bucket",
            &self.bucket,
            "--key",
            &format!("{}{key}", self.prefix),
            "--body",
            file.path().to_str().unwrap(),
        ]);
    }

    /// Upload an offline fixture using the CLI's bounded concurrent transfers.
    ///
    /// # Panics
    /// Panics if the directory path is not Unicode or a CLI transfer fails.
    pub fn upload_directory(&self, directory: &std::path::Path) {
        self.aws(&[
            "s3",
            "sync",
            directory.to_str().unwrap(),
            &format!("s3://{}/{}", self.bucket, self.prefix),
            "--only-show-errors",
        ]);
    }

    /// Create an isolated provider bucket and evidence directory.
    ///
    /// # Panics
    /// Panics on missing configuration, a non-loopback endpoint, or setup failure.
    #[must_use]
    pub fn new(phase: &str) -> Self {
        let endpoint = std::env::var("FITZ_S3_TEST_ENDPOINT").expect("loopback Sqrzl endpoint");
        assert!(
            endpoint.starts_with("http://127.0.0.1:"),
            "only disposable loopback provider allowed"
        );
        let id = format!("{phase}-{}", uuid::Uuid::new_v4());
        let directory =
            PathBuf::from(std::env::var("FITZ_S3_REPORT_DIR").expect("evidence directory"))
                .join(&id);
        std::fs::create_dir_all(&directory).unwrap();
        let campaign = Self {
            bucket: format!("fitz-{id}"),
            prefix: "qualification/".into(),
            endpoint,
            directory,
            id,
        };
        campaign.aws(&["s3api", "create-bucket", "--bucket", &campaign.bucket]);
        campaign
    }

    fn aws(&self, args: &[&str]) -> Vec<u8> {
        let result = Command::new("aws")
            .env("AWS_ACCESS_KEY_ID", "admin")
            .env("AWS_SECRET_ACCESS_KEY", "easy-peasy")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .args([
                "--endpoint-url",
                &self.endpoint,
                "--region",
                "us-east-1",
                "--no-cli-pager",
                "--cli-connect-timeout",
                "10",
                "--cli-read-timeout",
                "10",
            ])
            .args(args)
            .output()
            .expect("native S3 inventory request");
        assert!(
            result.status.success(),
            "S3 {args:?}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        result.stdout
    }

    /// Save and decode catalog authority alongside the remote object inventory.
    ///
    /// # Panics
    /// Panics if inventory, catalog decoding, or evidence persistence fails.
    pub fn snapshot(&self, phase: &str) -> Snapshot {
        let catalog_path = self.directory.join(format!("{phase}-catalog.json"));
        self.aws(&[
            "s3api",
            "get-object",
            "--bucket",
            &self.bucket,
            "--key",
            &format!("{}wal/publication-catalog.v1.json", self.prefix),
            catalog_path.to_str().unwrap(),
        ]);
        let catalog = cntryl_midge::__internal::wal::catalog::WalPublicationCatalog::decode(
            &std::fs::read(&catalog_path).unwrap(),
        )
        .unwrap();
        let segments = catalog.entries();
        let listing = self.aws(&[
            "s3api",
            "list-objects-v2",
            "--bucket",
            &self.bucket,
            "--prefix",
            &self.prefix,
            "--output",
            "json",
        ]);
        std::fs::write(
            self.directory.join(format!("{phase}-objects.json")),
            &listing,
        )
        .unwrap();
        let listing: Value = serde_json::from_slice(&listing).unwrap();
        let objects = listing["Contents"]
            .as_array()
            .expect("nonempty cloud store");
        let mut snapshot = Snapshot {
            serialized_catalog_bytes: std::fs::metadata(&catalog_path).unwrap().len(),
            catalog_bytes: segments
                .iter()
                .map(cntryl_midge::__internal::wal::catalog::CatalogEntry::size_bytes)
                .sum(),
            remote_wal_bytes: 0,
            remote_wal_objects: 0,
            sst_objects: 0,
            segment_ids: segments
                .iter()
                .map(cntryl_midge::__internal::wal::catalog::CatalogEntry::id)
                .collect(),
        };
        snapshot.segment_ids.sort_unstable();
        for object in objects {
            let key = object["Key"].as_str().unwrap();
            let extension = std::path::Path::new(key)
                .extension()
                .and_then(|value| value.to_str());
            if extension == Some("wal") {
                snapshot.remote_wal_bytes += object["Size"].as_u64().unwrap();
                snapshot.remote_wal_objects += 1;
            }
            if extension == Some("sst") {
                snapshot.sst_objects += 1;
            }
        }
        println!(
            "S3 {phase}: catalog_bytes={}, remote_wal_bytes={}, WAL objects={}, SSTs={}",
            snapshot.catalog_bytes,
            snapshot.remote_wal_bytes,
            snapshot.remote_wal_objects,
            snapshot.sst_objects
        );
        snapshot
    }

    /// Persist a workload report with runtime source and provider identity.
    ///
    /// # Panics
    /// Panics on missing identity configuration or evidence serialization/write failure.
    pub fn report(&self, measurements: &Value) {
        let report = json!({
            "source_sha": std::env::var("FITZ_RESOURCE_SOURCE_SHA").expect("source SHA"),
            "provider_image": std::env::var("FITZ_S3_PROVIDER_IMAGE").expect("pinned provider image"),
            "bucket": self.bucket, "prefix": self.prefix, "measurements": measurements,
        });
        std::fs::write(
            self.directory.join("report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
    }
}

pub struct Broker {
    pub name: String,
    http: String,
    tcp: SocketAddr,
    directory: PathBuf,
    sample: usize,
    stats: Option<Child>,
    removed: bool,
    readiness_started: Instant,
}

impl Broker {
    /// Start an isolated broker under the declared CPU, memory and swap limits.
    ///
    /// # Panics
    /// Panics on missing configuration, Docker failure, or incorrect container limits.
    #[must_use]
    pub fn start(campaign: &Campaign) -> Self {
        let name = format!("fitz-s3-{}-{}", campaign.id, uuid::Uuid::new_v4());
        let image = std::env::var("FITZ_TEST_IMAGE").expect("exact source runtime image");
        let network = std::env::var("FITZ_S3_NETWORK").expect("isolated provider network");
        let readiness_started = Instant::now();
        command(
            "docker",
            &[
                "run",
                "--detach",
                "--name",
                &name,
                "--network",
                &network,
                "--label",
                "fitz-s3-qualification=true",
                "--cpus",
                "0.25",
                "--memory",
                "512m",
                "--memory-swap",
                "512m",
                "--publish",
                "127.0.0.1::4090",
                "--publish",
                "127.0.0.1::4091",
                "--env",
                "FITZ_AUTH_REQUIRED=false",
                "--env",
                "FITZ_ADMIN_AUTH_MODE=open",
                "--env",
                "FITZ_STORAGE_MODE=cloud",
                "--env",
                "FITZ_STORAGE_PROVIDER=s3-compatible",
                "--env",
                "FITZ_STORAGE_ENDPOINT=http://fitz-s3-provider:9000",
                "--env",
                &format!("FITZ_STORAGE_BUCKET={}", campaign.bucket),
                "--env",
                &format!("FITZ_STORAGE_PREFIX={}", campaign.prefix),
                "--env",
                "FITZ_STORAGE_CACHE_PATH=/data",
                "--env",
                "FITZ_STORAGE_CLOUD_DURABILITY=strict",
                "--env",
                "FITZ_STORAGE_LEASE_TTL_SECS=59",
                "--env",
                "AWS_ACCESS_KEY_ID=admin",
                "--env",
                "AWS_SECRET_ACCESS_KEY=easy-peasy",
                "--env",
                "FITZ_LOG_LEVEL=info",
                "--env",
                "OTEL_ENABLED=false",
                &image,
            ],
        );
        let http = String::from_utf8(command("docker", &["port", &name, "4090/tcp"])).unwrap();
        let tcp = String::from_utf8(command("docker", &["port", &name, "4091/tcp"])).unwrap();
        let mut broker = Self {
            name,
            http: format!("http://{}", http.trim()),
            tcp: tcp.trim().parse().unwrap(),
            directory: campaign.directory.clone(),
            sample: 0,
            stats: None,
            removed: false,
            readiness_started,
        };
        let _ = inspect_container(&broker.name);
        broker.sample_memory();
        broker
    }

    #[must_use]
    pub fn address(&self) -> SocketAddr {
        self.tcp
    }

    #[must_use]
    pub fn endpoint(&self) -> String {
        self.http.clone()
    }

    fn capture(&mut self) {
        let suffix = self.sample;
        self.sample += 1;
        std::fs::write(
            self.directory
                .join(format!("{}-container-{suffix}.json", self.name)),
            command("docker", &["inspect", &self.name]),
        )
        .unwrap();
        let result = Command::new("docker")
            .args(["logs", &self.name])
            .output()
            .unwrap();
        let mut log = result.stdout;
        log.extend(result.stderr);
        std::fs::write(
            self.directory
                .join(format!("{}-broker-{suffix}.log", self.name)),
            log,
        )
        .unwrap();
    }

    fn sample_memory(&mut self) {
        let file = std::fs::File::create(
            self.directory
                .join(format!("{}-stats-{}.jsonl", self.name, self.sample)),
        )
        .unwrap();
        self.stats = Some(
            Command::new("docker")
                .args(["stats", "--format", "{{json .}}", &self.name])
                .stdout(Stdio::from(file))
                .spawn()
                .expect("sample capped broker memory"),
        );
    }

    fn stop_sampler(&mut self) {
        if let Some(mut child) = self.stats.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Wait for strict readiness, measured from this container's startup attempt.
    ///
    /// # Panics
    /// Panics if the broker exits, evidence capture fails, or readiness exceeds 180 seconds.
    pub async fn ready(&mut self) -> f64 {
        self.ready_since(self.readiness_started).await
    }

    /// Wait for strict readiness against an existing crash/start timestamp.
    ///
    /// # Panics
    /// Panics on invalid responses, process exit, evidence failure, or the 180-second deadline.
    pub async fn ready_since(&mut self, started: Instant) -> f64 {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        loop {
            let state: Vec<Value> =
                serde_json::from_slice(&command("docker", &["inspect", &self.name])).unwrap();
            if state[0]["State"]["Running"] != true {
                self.capture();
                panic!("broker exited before readiness: {}", state[0]["State"]);
            }
            let mut ready = false;
            if let Ok(response) = client.get(format!("{}/healthz", self.http)).send().await {
                if response.status().is_success() {
                    let value: Value = response.json().await.unwrap();
                    ready = value["status"] == "ready";
                }
            }
            let elapsed = started.elapsed();
            if elapsed >= Duration::from_secs(180) {
                self.capture();
                panic!("strict readiness exceeded 180 seconds, including startup and lease expiry");
            }
            if ready {
                let seconds = elapsed.as_secs_f64();
                self.capture();
                println!("S3 {} ready in {seconds:.3}s", self.name);
                return seconds;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Prepare an explicitly persisted fixture through orderly broker shutdown.
    ///
    /// # Panics
    /// Panics if shutdown does not exit successfully without an OOM kill.
    pub fn checkpoint(&mut self) {
        // Explicit fixture preparation, not an enqueue durability guarantee.
        self.capture();
        command(
            "docker",
            &["stop", "--signal", "SIGINT", "--timeout", "60", &self.name],
        );
        self.stop_sampler();
        let state: Vec<Value> =
            serde_json::from_slice(&command("docker", &["inspect", &self.name])).unwrap();
        assert_eq!(
            state[0]["State"]["ExitCode"], 0,
            "checkpoint must finish orderly storage shutdown"
        );
        assert_eq!(state[0]["State"]["OOMKilled"], false);
        self.capture();
    }

    /// Abruptly stop the broker and verify SIGKILL rather than an OOM kill.
    ///
    /// # Panics
    /// Panics on Docker/evidence failure or an unexpected exit status.
    pub fn crash(&mut self) {
        self.capture();
        command("docker", &["kill", "--signal", "KILL", &self.name]);
        self.stop_sampler();
        let state: Vec<Value> =
            serde_json::from_slice(&command("docker", &["inspect", &self.name])).unwrap();
        assert_eq!(state[0]["State"]["ExitCode"], 137);
        assert_eq!(state[0]["State"]["OOMKilled"], false);
        self.capture();
    }

    /// Restart the retained container and original cache volume.
    ///
    /// # Panics
    /// Panics on Docker failure or invalid published endpoint addresses.
    pub fn restart(&mut self) {
        self.readiness_started = Instant::now();
        command("docker", &["start", &self.name]);
        self.http = format!(
            "http://{}",
            String::from_utf8(command("docker", &["port", &self.name, "4090/tcp"]))
                .unwrap()
                .trim()
        );
        self.tcp = String::from_utf8(command("docker", &["port", &self.name, "4091/tcp"]))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        self.sample_memory();
    }

    /// Capture evidence and remove this broker and its anonymous cache volume.
    ///
    /// # Panics
    /// Panics if evidence capture or Docker removal fails.
    pub fn remove(&mut self) {
        self.capture();
        self.stop_sampler();
        command("docker", &["rm", "--force", "--volumes", &self.name]);
        self.removed = true;
    }
}

/// Verify the deployed resource envelope and absence of an OOM or automatic restart.
///
/// # Panics
/// Panics if Docker inspection fails or any declared resource/lifecycle invariant differs.
#[must_use]
pub fn inspect_container(name: &str) -> Value {
    let containers: Vec<Value> =
        serde_json::from_slice(&command("docker", &["inspect", name])).unwrap();
    let container = containers.into_iter().next().expect("broker container");
    assert_eq!(container["HostConfig"]["NanoCpus"], 250_000_000);
    assert_eq!(container["HostConfig"]["Memory"], 536_870_912);
    assert_eq!(container["HostConfig"]["MemorySwap"], 536_870_912);
    assert_eq!(container["State"]["Running"], true);
    assert_eq!(container["State"]["OOMKilled"], false);
    assert_eq!(container["RestartCount"], 0);
    container
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.stop_sampler();
        if !self.removed {
            // Retain failed stores, but stop their writers so later isolated
            // campaigns do not compete with abandoned maintenance workloads.
            for (action, extension) in [("inspect", "json"), ("logs", "log")] {
                if let Ok(result) = Command::new("docker").args([action, &self.name]).output() {
                    let mut bytes = result.stdout;
                    bytes.extend(result.stderr);
                    let _ = std::fs::write(
                        self.directory
                            .join(format!("{}-failed-{action}.{extension}", self.name)),
                        bytes,
                    );
                }
            }
            let _ = Command::new("docker")
                .args(["kill", "--signal", "KILL", &self.name])
                .output();
        }
    }
}
