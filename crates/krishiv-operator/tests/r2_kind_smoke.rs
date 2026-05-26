#![forbid(unsafe_code)]

use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

const KIND_ENV: &str = "KRISHIV_KIND_E2E";
const DEFAULT_CLUSTER: &str = "krishiv-r2";
const DEFAULT_NAMESPACE: &str = "krishiv-system";
const DEFAULT_TIMEOUT_SECS: u64 = 90;

#[test]
fn kind_smoke_submits_batch_krishivjob() {
    let Some(config) = KindSmokeConfig::from_env() else {
        return;
    };

    config.ensure_cluster_and_manifests();
    config.wait_for_phase("sample-batch", &["Running", "Succeeded"]);
    config.assert_task_counter("sample-batch", "assigned", "2");
}

#[test]
fn kind_smoke_submits_streaming_krishivjob() {
    let Some(config) = KindSmokeConfig::from_env() else {
        return;
    };

    config.ensure_cluster_and_manifests();
    config.wait_for_phase("sample-streaming", &["Running", "Succeeded"]);
    config.assert_task_counter("sample-streaming", "assigned", "1");
}

#[derive(Debug, Clone)]
struct KindSmokeConfig {
    cluster: String,
    namespace: String,
    timeout: Duration,
    repo_root: PathBuf,
    create_cluster: bool,
    image: Option<String>,
    load_image: bool,
}

impl KindSmokeConfig {
    fn from_env() -> Option<Self> {
        if env::var(KIND_ENV).ok().as_deref() != Some("1") {
            eprintln!("skipping kind smoke test; set {KIND_ENV}=1 to enable");
            return None;
        }

        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("operator crate lives under crates/")
            .to_path_buf();
        let timeout = env::var("KRISHIV_KIND_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TIMEOUT_SECS);

        Some(Self {
            cluster: env::var("KRISHIV_KIND_CLUSTER").unwrap_or_else(|_| DEFAULT_CLUSTER.into()),
            namespace: env::var("KRISHIV_KIND_NAMESPACE")
                .unwrap_or_else(|_| DEFAULT_NAMESPACE.into()),
            timeout: Duration::from_secs(timeout),
            repo_root,
            create_cluster: env::var("KRISHIV_KIND_SKIP_CREATE").ok().as_deref() != Some("1"),
            image: env::var("KRISHIV_KIND_IMAGE").ok(),
            load_image: env::var("KRISHIV_KIND_SKIP_LOAD_IMAGE").ok().as_deref() != Some("1"),
        })
    }

    fn ensure_cluster_and_manifests(&self) {
        self.require_command("kind", &["version"]);
        self.require_command("kubectl", &["version", "--client"]);

        if self.create_cluster && !self.kind_cluster_exists() {
            self.run("kind", &["create", "cluster", "--name", &self.cluster]);
        }
        if let Some(image) = &self.image {
            let load = self.load_image;
            if load {
                self.run(
                    "kind",
                    &["load", "docker-image", image, "--name", &self.cluster],
                );
            }
        }

        self.run(
            "kubectl",
            &["config", "use-context", &format!("kind-{}", self.cluster)],
        );
        self.run("kubectl", &["apply", "-k", "k8s/manifests"]);
        if let Some(image) = &self.image {
            self.set_deployment_image("krishiv-operator", "operator", image);
        }
        self.wait_for_crd();
        self.wait_for_operator();
    }

    fn wait_for_phase(&self, job_name: &str, phases: &[&str]) {
        let expression = "{.status.phase}";
        let deadline = Instant::now() + self.timeout;
        let mut last = String::new();

        while Instant::now() < deadline {
            last = self.kubectl_jsonpath(job_name, expression);
            if phases.iter().any(|phase| *phase == last) {
                return;
            }
            thread::sleep(Duration::from_secs(2));
        }

        panic!("KrishivJob {job_name} did not reach {phases:?}; last phase was `{last}`");
    }

    fn assert_task_counter(&self, job_name: &str, counter: &str, expected: &str) {
        let expression = format!("{{.status.tasks.{counter}}}");
        let actual = self.kubectl_jsonpath(job_name, &expression);

        assert_eq!(
            actual, expected,
            "unexpected .status.tasks.{counter} for {job_name}"
        );
    }

    fn wait_for_crd(&self) {
        self.run(
            "kubectl",
            &[
                "wait",
                "--for=condition=Established",
                "crd/krishivjobs.krishiv.io",
                "--timeout=60s",
            ],
        );
    }

    fn wait_for_operator(&self) {
        self.run(
            "kubectl",
            &[
                "-n",
                &self.namespace,
                "wait",
                "--for=condition=Available",
                "deployment/krishiv-operator",
                "--timeout=90s",
            ],
        );
    }

    fn set_deployment_image(&self, deployment: &str, container: &str, image: &str) {
        self.run(
            "kubectl",
            &[
                "-n",
                &self.namespace,
                "set",
                "image",
                &format!("deployment/{deployment}"),
                &format!("{container}={image}"),
            ],
        );
    }

    fn kubectl_jsonpath(&self, job_name: &str, expression: &str) -> String {
        let output = Command::new("kubectl")
            .current_dir(&self.repo_root)
            .args([
                "-n",
                &self.namespace,
                "get",
                "krishivjob",
                job_name,
                "-o",
                &format!("jsonpath={expression}"),
            ])
            .output()
            .expect("kubectl jsonpath command should start");

        if !output.status.success() {
            return String::new();
        }

        String::from_utf8(output.stdout)
            .expect("kubectl output should be utf-8")
            .trim()
            .to_owned()
    }

    fn kind_cluster_exists(&self) -> bool {
        let output = Command::new("kind")
            .current_dir(&self.repo_root)
            .args(["get", "clusters"])
            .output()
            .expect("kind get clusters should start");

        output.status.success()
            && String::from_utf8(output.stdout)
                .expect("kind output should be utf-8")
                .lines()
                .any(|line| line == self.cluster)
    }

    fn require_command(&self, command: &str, args: &[&str]) {
        let output = Command::new(command)
            .args(args)
            .output()
            .unwrap_or_else(|error| panic!("{command} is required for kind smoke tests: {error}"));

        assert!(
            output.status.success(),
            "{command} {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn run(&self, command: &str, args: &[&str]) {
        let output = Command::new(command)
            .current_dir(&self.repo_root)
            .args(args)
            .output()
            .unwrap_or_else(|error| {
                panic!("{command} {} failed to start: {error}", args.join(" "))
            });

        assert!(
            output.status.success(),
            "{command} {} failed\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
