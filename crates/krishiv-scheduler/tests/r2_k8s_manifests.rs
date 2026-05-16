#![forbid(unsafe_code)]

const CRD: &str = include_str!("../../../k8s/crds/krishivjobs.yaml");
const KUSTOMIZATION: &str = include_str!("../../../k8s/manifests/kustomization.yaml");
const COORDINATOR_DEPLOYMENT: &str =
    include_str!("../../../k8s/manifests/coordinator-deployment.yaml");
const EXECUTOR_DEPLOYMENT: &str = include_str!("../../../k8s/manifests/executor-deployment.yaml");
const OPERATOR_DEPLOYMENT: &str = include_str!("../../../k8s/manifests/operator-deployment.yaml");
const RBAC: &str = include_str!("../../../k8s/manifests/rbac.yaml");
const SAMPLE_JOB: &str = include_str!("../../../k8s/manifests/sample-krishivjob.yaml");

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
            "coordinator-deployment.yaml",
            "coordinator-service.yaml",
            "executor-deployment.yaml",
            "sample-krishivjob.yaml",
        ],
    );
}

#[test]
fn coordinator_manifest_keeps_one_active_coordinator() {
    assert_contains_all(
        COORDINATOR_DEPLOYMENT,
        &[
            "kind: Deployment",
            "name: krishiv-coordinator",
            "app.kubernetes.io/component: coordinator",
            "replicas: 1",
            "serviceAccountName: krishiv-controller",
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
            "replicas: 1",
            "serviceAccountName: krishiv-controller",
            "- krishiv-operator",
            "--namespace",
            "--coordinator-id",
            "--bootstrap-executor-slots",
            "KRISHIV_COORDINATOR_ID",
            "KRISHIV_NAMESPACE",
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
            "KRISHIV_EXECUTOR_ID",
            "KRISHIV_TASK_SLOTS",
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

fn assert_contains_all(haystack: &str, needles: &[&str]) {
    for needle in needles {
        assert!(
            haystack.contains(needle),
            "expected manifest to contain `{needle}`"
        );
    }
}
