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
pub(super) struct Snapshot {
    pub catalog_bytes: u64,
    pub remote_wal_bytes: u64,
    pub remote_wal_objects: usize,
    pub sst_objects: usize,
    pub segment_ids: Vec<u64>,
}

pub(super) struct Campaign {
    pub id: String,
    pub directory: PathBuf,
    bucket: String,
    prefix: String,
    endpoint: String,
}

impl Campaign {
    #[cfg(feature = "recovery-qualification")]
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
        let catalog: Value =
            serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
        let segments = catalog["segments"]
            .as_object()
            .expect("authoritative WAL segments");
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
            catalog_bytes: segments
                .values()
                .map(|segment| segment["size_bytes"].as_u64().unwrap())
                .sum(),
            remote_wal_bytes: 0,
            remote_wal_objects: 0,
            sst_objects: 0,
            segment_ids: segments.keys().map(|id| id.parse().unwrap()).collect(),
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

pub(super) struct Broker {
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
                "FITZ_QUEUE_WRITE_POLICY=strict",
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
        super::super::inspect_container(&broker.name);
        broker.sample_memory();
        broker
    }

    pub fn address(&self) -> SocketAddr {
        self.tcp
    }

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

    pub async fn ready(&mut self) -> f64 {
        let started = self.readiness_started;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        loop {
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

    pub fn remove(&mut self) {
        self.capture();
        self.stop_sampler();
        command("docker", &["rm", "--force", "--volumes", &self.name]);
        self.removed = true;
    }
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
