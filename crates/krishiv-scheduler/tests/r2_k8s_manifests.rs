#![forbid(unsafe_code)]

const CRD: &str = include_str!("../../../k8s/crds/krishivjobs.yaml");
const KUSTOMIZATION: &str = include_str!("../../../k8s/manifests/kustomization.yaml");
const NETWORK_POLICY: &str = include_str!("../../../k8s/manifests/network-policy.yaml");
const COORDINATOR_SERVICE: &str = include_str!("../../../k8s/manifests/coordinator-service.yaml");
const EXECUTOR_DEPLOYMENT: &str = include_str!("../../../k8s/manifests/executor-deployment.yaml");
const OPERATOR_DEPLOYMENT: &str = include_str!("../../../k8s/manifests/operator-deployment.yaml");
const RBAC: &str = include_str!("../../../k8s/manifests/rbac.yaml");
const SAMPLE_JOB: &str = include_str!("../../../k8s/manifests/sample-krishivjob.yaml");
const SAMPLE_STREAMING_JOB: &str =
    include_str!("../../../k8s/manifests/sample-streaming-krishivjob.yaml");

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
            "../crds/krishivjobs.yaml",
            "namespace.yaml",
            "serviceaccount.yaml",
            "rbac.yaml",
            "operator-deployment.yaml",
            "coordinator-service.yaml",
            "executor-deployment.yaml",
            "sample-krishivjob.yaml",
            "sample-streaming-krishivjob.yaml",
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
            "port: 9090",
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
            "replicas: 2",
            "serviceAccountName: krishiv-controller",
            "- krishiv-operator",
            "--status-addr",
            "--executor-grpc-addr",
            "0.0.0.0:8080",
            "0.0.0.0:9090",
            "name: grpc",
            "readinessProbe:",
            "livenessProbe:",
            "KRISHIV_COORDINATOR_ID",
        ],
    );
}

#[test]
fn operator_manifest_watches_jobs_and_patches_status() {
    assert_contains_all(
        OPERATOR_DEPLOYMENT,
        &[
            "kind: Deployment",
            "name: krishiv-operator",
            "app.kubernetes.io/component: operator",
            "replicas: 2",
            "serviceAccountName: krishiv-controller",
            "- krishiv-operator",
            "--namespace",
            "--coordinator-id",
            "--bootstrap-executor-slots",
            "--status-addr",
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
            "port: 8080",
            "targetPort: http",
            "name: grpc",
            "port: 9090",
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
            "http://krishiv-coordinator.krishiv-system.svc:9090",
            "--connect",
            "--heartbeat-interval-secs",
        ],
    );
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

fn assert_contains_all(haystack: &str, needles: &[&str]) {
    for needle in needles {
        assert!(
            haystack.contains(needle),
            "expected manifest to contain `{needle}`"
        );
    }
}
