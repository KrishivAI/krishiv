#![forbid(unsafe_code)]

const CRD: &str = include_str!("../../../deploy/k8s/crds/krishivjobs.yaml");
const KUSTOMIZATION: &str = include_str!("../../../deploy/k8s/operator/kustomization.yaml");
const NETWORK_POLICY: &str = include_str!("../../../deploy/k8s/operator/network-policy.yaml");
const COORDINATOR_SERVICE: &str =
    include_str!("../../../deploy/k8s/operator/coordinator-service.yaml");
const EXECUTOR_DEPLOYMENT: &str =
    include_str!("../../../deploy/k8s/operator/executor-deployment.yaml");
const OPERATOR_DEPLOYMENT: &str =
    include_str!("../../../deploy/k8s/operator/operator-deployment.yaml");
const RBAC: &str = include_str!("../../../deploy/k8s/operator/rbac.yaml");
const SAMPLE_JOB: &str = include_str!("../../../deploy/k8s/operator/samples/krishivjob-batch.yaml");
const SAMPLE_STREAMING_JOB: &str =
    include_str!("../../../deploy/k8s/operator/samples/krishivjob-streaming.yaml");
const DIRECT_DISTRIBUTED: &str =
    include_str!("../../../deploy/k8s/direct/krishiv-distributed.yaml");
const HELM_VALUES: &str = include_str!("../../../deploy/k8s/helm/krishiv/values.yaml");
const HELM_COORDINATOR_DEPLOYMENT: &str =
    include_str!("../../../deploy/k8s/helm/krishiv/templates/coordinator-deployment.yaml");
const HELM_EXECUTOR_DEPLOYMENT: &str =
    include_str!("../../../deploy/k8s/helm/krishiv/templates/executor-deployment.yaml");
const K8S_README: &str = include_str!("../../../deploy/k8s/README.md");
const COORDINATOR_DAEMON: &str = include_str!("../src/coordinator_daemon.rs");

#[test]
fn krishivjob_crd_declares_expected_api_shape() {
    assert_contains_all(
        CRD,
        &[
            "apiVersion: apiextensions.k8s.io/v1",
            "kind: CustomResourceDefinition",
            "name: krishivjobs.krishiv.io",
            "group: krishiv.io",
            "plural: krishivjobs",
            "kind: KrishivJob",
            "name: v1alpha1",
            "subresources:",
            "status: {}",
            "openAPIV3Schema:",
            "required:",
            "- spec",
            "mode:",
            "- batch",
            "- streaming",
            "minimum: 1",
            "phase:",
            "- Accepted",
            "- Running",
            "- Succeeded",
            "- Failed",
        ],
    );
}

#[test]
fn kustomization_references_all_r2_manifests() {
    assert_contains_all(
        KUSTOMIZATION,
        &[
            "../crds",
            "namespace.yaml",
            "serviceaccount.yaml",
            "rbac.yaml",
            "operator-deployment.yaml",
            "coordinator-service.yaml",
            "executor-deployment.yaml",
            "samples/krishivjob-batch.yaml",
            "samples/krishivjob-streaming.yaml",
            "network-policy.yaml",
        ],
    );
}

#[test]
fn network_policy_restricts_coordinator_grpc_to_krishiv_namespace() {
    assert_contains_all(
        NETWORK_POLICY,
        &[
            "kind: NetworkPolicy",
            "name: krishiv-coordinator-grpc",
            "namespace: krishiv-system",
            "app.kubernetes.io/component: operator",
            "Ingress",
            "kubernetes.io/metadata.name: krishiv-system",
            "port: 2001",
        ],
    );
}

#[test]
fn operator_manifest_runs_one_active_coordinator_runtime() {
    assert_contains_all(
        OPERATOR_DEPLOYMENT,
        &[
            "kind: Deployment",
            "name: krishiv-operator",
            "app.kubernetes.io/component: operator",
            "replicas: 1",
            "serviceAccountName: krishiv-controller",
            "- krishiv-operator",
            "--executor-grpc-addr",
            "0.0.0.0:2001",
            "name: grpc",
            "name: http",
            "KRISHIV_COORDINATOR_BEARER_TOKEN",
            "KRISHIV_COORDINATOR_BEARER_TOKENS",
            "KRISHIV_COORDINATOR_BEARER_TOKEN_FILE",
            "KRISHIV_COORDINATOR_BEARER_TOKENS_FILE",
            "KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS",
            "/var/run/krishiv/coordinator-auth/token",
            "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN",
            "krishiv-coordinator-auth",
            "krishiv-executor-task-auth",
            "key: tokens",
            "optional: true",
            "name: coordinator-auth",
            "secretName: krishiv-coordinator-auth",
            "readOnly: true",
            "KRISHIV_COORDINATOR_ID",
        ],
    );
    assert!(!OPERATOR_DEPLOYMENT.contains("KRISHIV_ALLOW_ANONYMOUS"));
}

#[test]
fn operator_manifest_watches_jobs_and_patches_status() {
    assert_contains_all(
        OPERATOR_DEPLOYMENT,
        &[
            "kind: Deployment",
            "name: krishiv-operator",
            "app.kubernetes.io/component: operator",
            "replicas: 1",
            "serviceAccountName: krishiv-controller",
            "- krishiv-operator",
            "--namespace",
            "--coordinator-id",
            "--executor-grpc-addr",
            "KRISHIV_COORDINATOR_ID",
            "KRISHIV_NAMESPACE",
        ],
    );
}

#[test]
fn coordinator_service_exposes_operator_owned_status_runtime() {
    assert_contains_all(
        COORDINATOR_SERVICE,
        &[
            "kind: Service",
            "name: krishiv-coordinator",
            "app.kubernetes.io/component: operator",
            "name: http",
            "port: 2002",
            "targetPort: http",
            "name: grpc",
            "port: 2001",
            "targetPort: grpc",
        ],
    );
}

#[test]
fn executor_manifest_declares_replaceable_executors() {
    assert_contains_all(
        EXECUTOR_DEPLOYMENT,
        &[
            "kind: Deployment",
            "name: krishiv-executor",
            "app.kubernetes.io/component: executor",
            "replicas: 2",
            "krishiv-executor",
            "KRISHIV_EXECUTOR_ID",
            "KRISHIV_TASK_SLOTS",
            "KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH",
            "KRISHIV_COORDINATOR_BEARER_TOKEN",
            "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN",
            "krishiv-coordinator-auth",
            "krishiv-executor-task-auth",
            "http://krishiv-coordinator.krishiv-system.svc:2001",
            "--connect",
            "--heartbeat-interval-secs",
        ],
    );
}

#[test]
fn direct_distributed_manifest_requires_executor_task_auth_secret() {
    assert_contains_all(
        DIRECT_DISTRIBUTED,
        &[
            "krishiv-executor-task-auth",
            "krishiv-coordinator-auth",
            "KRISHIV_COORDINATOR_BEARER_TOKEN",
            "KRISHIV_COORDINATOR_BEARER_TOKENS",
            "KRISHIV_COORDINATOR_BEARER_TOKEN_FILE",
            "KRISHIV_COORDINATOR_BEARER_TOKENS_FILE",
            "KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS",
            "/var/run/krishiv/coordinator-auth/token",
            "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN",
            "KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH",
            "secretKeyRef:",
            "key: token",
            "key: tokens",
            "optional: true",
            "name: coordinator-auth",
            "secretName: krishiv-coordinator-auth",
            "readOnly: true",
            "Secret krishiv-system/krishiv-executor-task-auth",
        ],
    );
    assert!(!DIRECT_DISTRIBUTED.contains("--insecure"));
}

#[test]
fn helm_chart_uses_current_daemon_flags_and_task_auth_secret() {
    assert_contains_all(
        HELM_VALUES,
        &[
            "coordinatorAuth:",
            "executorTaskAuth:",
            "secretName:",
            "secretKey:",
            "rotationSecretKey:",
            "mountPath:",
            "reloadIntervalSeconds:",
        ],
    );
    assert_contains_all(
        HELM_COORDINATOR_DEPLOYMENT,
        &[
            "/usr/local/bin/krishiv",
            "coordinator",
            "--grpc-addr",
            "KRISHIV_COORDINATOR_BEARER_TOKEN",
            "KRISHIV_COORDINATOR_BEARER_TOKENS",
            "KRISHIV_COORDINATOR_BEARER_TOKEN_FILE",
            "KRISHIV_COORDINATOR_BEARER_TOKENS_FILE",
            "KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS",
            "KRISHIV_COORDINATOR_ID",
            "path: /leaderz",
            "type: RollingUpdate",
            "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN",
            "secretKeyRef:",
            "rotationSecretKey",
            "coordinatorAuth.mountPath",
            "coordinatorAuth.reloadIntervalSeconds",
            "name: coordinator-auth",
            "readOnly: true",
            "optional: true",
            "httpGet:",
        ],
    );
    assert_contains_all(
        HELM_EXECUTOR_DEPLOYMENT,
        &[
            "/usr/local/bin/krishiv",
            "executor",
            "--coordinator",
            "--connect",
            "--task-grpc-addr",
            "KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH",
            "KRISHIV_COORDINATOR_BEARER_TOKEN",
            "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN",
            "secretKeyRef:",
            "POD_IP",
            "httpGet:",
        ],
    );
    assert!(!HELM_COORDINATOR_DEPLOYMENT.contains("--listen"));
    assert!(!HELM_EXECUTOR_DEPLOYMENT.contains("--listen"));
    assert!(!HELM_COORDINATOR_DEPLOYMENT.contains("--insecure"));
}

#[test]
fn rbac_can_watch_jobs_and_update_status() {
    assert_contains_all(
        RBAC,
        &[
            "kind: ClusterRole",
            "krishivjobs",
            "krishivjobs/status",
            "- get",
            "- list",
            "- watch",
            "- update",
            "- patch",
            "kind: ClusterRoleBinding",
            "name: krishiv-controller",
        ],
    );
}

#[test]
fn sample_krishivjob_uses_v1alpha1_contract() {
    assert_contains_all(
        SAMPLE_JOB,
        &[
            "apiVersion: krishiv.io/v1alpha1",
            "kind: KrishivJob",
            "name: sample-batch",
            "namespace: krishiv-system",
            "mode: batch",
            "tasks: 2",
            "parallelism: 2",
            "restartPolicy: Never",
        ],
    );
}

#[test]
fn sample_streaming_krishivjob_uses_v1alpha1_contract() {
    assert_contains_all(
        SAMPLE_STREAMING_JOB,
        &[
            "apiVersion: krishiv.io/v1alpha1",
            "kind: KrishivJob",
            "name: sample-streaming",
            "namespace: krishiv-system",
            "mode: streaming",
            "tasks: 1",
            "parallelism: 1",
            "restartPolicy: Never",
        ],
    );
}

#[test]
fn deployment_conformance_k8s_kind_and_bare_metal_process_modes_are_declared() {
    assert_contains_all(
        K8S_README,
        &[
            "kind",
            "KRISHIV_KIND_E2E=1",
            "cargo test -p krishiv-operator --test r2_kind_smoke",
            "kubectl apply -k k8s/operator",
            "kubectl apply -f k8s/direct/krishiv-dev.yaml",
            "helm install krishiv k8s/helm/krishiv",
            "kubectl create secret generic krishiv-coordinator-auth",
            "kubectl create secret generic krishiv-executor-task-auth",
        ],
    );
    assert_contains_all(
        COORDINATOR_DAEMON,
        &[
            "bare metal + VM",
            "CoordinatorDaemonConfig",
            "build_shared_coordinator",
            "run_cluster_control_plane",
            "run_standalone_coordinator",
            "metadata_backend",
            "RocksDbMetadataStore",
            "EtcdMetadataStore",
            "leader_backend",
            "serve_coordinator_executor_grpc_with_listener",
        ],
    );
}

fn assert_contains_all(haystack: &str, needles: &[&str]) {
    for needle in needles {
        assert!(
            haystack.contains(needle),
            "expected manifest to contain `{needle}`"
        );
    }
}
