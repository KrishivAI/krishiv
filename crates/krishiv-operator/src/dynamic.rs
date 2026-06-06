//! Dynamic API helpers.

use kube::Client;
use kube::api::{Api, Patch, PatchParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use serde_json::{Value, json};

use crate::constants::{API_GROUP, API_VERSION, FIELD_MANAGER, FINALIZER, KIND};
use crate::crd::job::{KrishivJobResource, KrishivJobStatus};
use crate::error::OperatorResult;

/// Convert a Kubernetes dynamic object into a typed `KrishivJobResource`.
pub fn resource_from_dynamic_object(object: &DynamicObject) -> OperatorResult<KrishivJobResource> {
    let value = serde_json::to_value(object)?;
    let mut resource: KrishivJobResource = serde_json::from_value(value)?;
    if resource.api_version.is_empty() {
        resource.api_version = format!("{API_GROUP}/{API_VERSION}");
    }
    if resource.kind.is_empty() {
        resource.kind = KIND.to_owned();
    }
    Ok(resource)
}

/// Patch `metadata.finalizers` to include the Krishiv job finalizer (P0-6).
pub async fn patch_krishivjob_finalizer(
    jobs: &Api<DynamicObject>,
    resource: &KrishivJobResource,
) -> OperatorResult<()> {
    let mut finalizers = resource.metadata.finalizers.clone();
    if !finalizers.iter().any(|f| f == FINALIZER) {
        finalizers.push(FINALIZER.to_string());
    }
    let patch = json!({ "metadata": { "finalizers": finalizers } });
    let params = PatchParams::default();
    jobs.patch(&resource.metadata.name, &params, &Patch::Merge(&patch))
        .await?;
    Ok(())
}

pub async fn remove_krishivjob_finalizer(
    jobs: &Api<DynamicObject>,
    resource: &KrishivJobResource,
) -> OperatorResult<()> {
    let finalizers: Vec<String> = resource
        .metadata
        .finalizers
        .iter()
        .filter(|finalizer| finalizer.as_str() != FINALIZER)
        .cloned()
        .collect();
    let patch = json!({ "metadata": { "finalizers": finalizers } });
    let params = PatchParams::default();
    jobs.patch(&resource.metadata.name, &params, &Patch::Merge(&patch))
        .await?;
    Ok(())
}

pub async fn patch_krishivjob_status(
    jobs: &Api<DynamicObject>,
    resource: &KrishivJobResource,
    status: &KrishivJobStatus,
) -> OperatorResult<()> {
    let params = PatchParams::apply(FIELD_MANAGER).force();
    // SSA requires apiVersion/kind/metadata so the server can track field
    // ownership; the status subresource ignores spec fields in the document.
    let doc = json!({
        "apiVersion": format!("{API_GROUP}/{API_VERSION}"),
        "kind": KIND,
        "metadata": { "name": &resource.metadata.name },
        "status": status,
    });
    jobs.patch_status(&resource.metadata.name, &params, &Patch::Apply(doc))
        .await?;
    Ok(())
}

/// Build the Kubernetes status merge patch.
pub fn status_patch(status: &KrishivJobStatus) -> Value {
    json!({ "status": status })
}

/// API resource descriptor for `krishivjobs.krishiv.io`.
pub fn krishivjob_api_resource() -> ApiResource {
    let gvk = GroupVersionKind::gvk(API_GROUP, API_VERSION, KIND);
    ApiResource::from_gvk_with_plural(&gvk, "krishivjobs")
}

/// Kubernetes API handle for `KrishivJob` dynamic objects.
pub fn krishivjob_api(
    client: Client,
    namespace: Option<&str>,
) -> OperatorResult<Api<DynamicObject>> {
    let api_resource = krishivjob_api_resource();
    Ok(match namespace {
        Some(namespace) => Api::namespaced_with(client, namespace, &api_resource),
        None => Api::all_with(client, &api_resource),
    })
}
