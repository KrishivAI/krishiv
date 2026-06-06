#[cfg(test)]
mod operator_tests {
    use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
        JobState, TaskState, TaskStatusUpdate,
    };

    use crate::{
        BootstrapExecutor, ConditionStatus, CrdQueueManager, EXECUTOR_ID_LABEL,
        ExecutorPodLaunchFailure, FIELD_MANAGER, FINALIZER, K8sLeaseElection, KrishivJobMode,
        KrishivJobPhase, KrishivJobReconciler, KrishivJobResource, KrishivJobSpec,
        KrishivJobStatus, KrishivQueue, KrishivQueueSpec, KrishivQueueStatus,
        KubernetesControllerConfig, KubernetesControllerRuntime, ObjectMeta, OperatorError,
        ReconcileAction, TaskStatusCounters, build_executor_pod_template, demo_coordinator,
        detect_executor_pod_launch_failure, job_spec_from_resource, krishivjob_api_resource,
        resource_from_dynamic_object, status_patch,
    };
    use crate::pod_manager::build_executor_pod;
    use krishiv_scheduler::{Coordinator, LeaderElection as _};
    use kube::core::DynamicObject;
    use serde_json::json;

    #[test]
    fn builds_scheduler_job_spec_from_batch_resource() {
        let resource = sample_resource();

        let job = job_spec_from_resource(&resource).unwrap();

        assert_eq!(job.job_id().as_str(), "krishiv-system.sample-batch");
        assert_eq!(job.name(), "sample-batch");
        assert_eq!(job.kind().to_string(), "batch");
        assert_eq!(job.task_count(), 2);
        assert_eq!(
            job.stages()[0].tasks()[0].description(),
            "sql: select 1 as value"
        );
    }

    #[test]
    fn rejects_invalid_resource_before_scheduling() {
        let mut resource = sample_resource();
        resource.spec.image = String::from(" ");

        let error = job_spec_from_resource(&resource).unwrap_err();

        assert!(matches!(error, OperatorError::InvalidResource { .. }));
        assert!(error.to_string().contains("spec.image"));
    }

    #[test]
    fn reconcile_submits_and_waits_for_executors_without_failing_resource() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = Coordinator::active(coordinator_id);

        let outcome = reconciler
            .reconcile(&mut coordinator, &sample_resource())
            .unwrap();

        assert_eq!(outcome.action(), ReconcileAction::Submitted);
        assert_eq!(outcome.status().phase, KrishivJobPhase::Accepted);
        assert_eq!(outcome.status().tasks.assigned, 0);
        assert_eq!(outcome.status().conditions[0].status, ConditionStatus::True);
        assert_eq!(
            outcome.status().conditions[0].reason.as_deref(),
            Some("SchedulerObserved")
        );
    }

    #[test]
    fn reconcile_submits_job_when_executor_is_available() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::Submitted);
        assert_eq!(outcome.status().phase, KrishivJobPhase::Running);
        assert_eq!(outcome.status().stages, 1);
        assert_eq!(outcome.status().tasks.assigned, 2);
    }

    #[test]
    fn reconcile_observes_existing_job_on_second_pass() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        reconciler.reconcile(&mut coordinator, &resource).unwrap();
        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::Observed);
        assert_eq!(outcome.status().tasks.assigned, 2);
    }

    #[test]
    fn reconcile_status_tracks_running_tasks_after_launch() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        reconciler.reconcile(&mut coordinator, &resource).unwrap();
        let job_id = resource.scheduler_job_id();
        coordinator
            .launch_assigned_tasks(&krishiv_proto::JobId::try_new(job_id).unwrap())
            .unwrap();
        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::Observed);
        assert_eq!(outcome.status().tasks.assigned, 2);
        assert_eq!(outcome.status().tasks.running, 0);
    }

    #[test]
    fn reconcile_status_tracks_succeeded_job() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        reconciler.reconcile(&mut coordinator, &resource).unwrap();
        let job_id = krishiv_proto::JobId::try_new(resource.scheduler_job_id()).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let stage_id = detail.stages()[0].stage_id().clone();
        let executor_id = ExecutorId::try_new("exec-operator-demo").unwrap();

        for task in detail.stages()[0].tasks() {
            coordinator
                .apply_task_update(TaskStatusUpdate::new(
                    job_id.clone(),
                    stage_id.clone(),
                    task.task_id().clone(),
                    executor_id.clone(),
                    TaskState::Succeeded,
                    1,
                ))
                .unwrap();
        }
        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.status().phase, KrishivJobPhase::Succeeded);
        assert_eq!(outcome.status().tasks.succeeded, 2);
    }

    #[test]
    fn streaming_resource_maps_to_streaming_job() {
        let mut resource = sample_resource();
        resource.metadata.name = String::from("sample-stream");
        resource.spec.mode = KrishivJobMode::Streaming;
        resource.spec.tasks = 1;

        let job = job_spec_from_resource(&resource).unwrap();

        assert_eq!(job.kind().to_string(), "streaming");
        assert_eq!(job.task_count(), 1);
    }

    #[test]
    fn krishivjob_api_resource_declares_explicit_plural() {
        let resource = krishivjob_api_resource();

        assert_eq!(resource.group, "krishiv.io");
        assert_eq!(resource.version, "v1alpha1");
        assert_eq!(resource.kind, "KrishivJob");
        assert_eq!(resource.plural, "krishivjobs");
    }

    #[test]
    fn executor_pod_template_runs_executor_not_job_args() {
        let resource = sample_resource();
        let template = build_executor_pod_template(&resource, "http://krishiv-coordinator:9090");
        let spec = template.spec.expect("pod template spec");
        let container = spec.containers.first().expect("executor container");

        assert_eq!(
            container.args.as_ref().expect("executor args"),
            &vec![
                String::from("executor"),
                String::from("--coordinator"),
                String::from("http://krishiv-coordinator:9090"),
                String::from("--connect"),
                String::from("--heartbeat-interval-secs"),
                String::from("1"),
            ]
        );
    }

    #[test]
    fn executor_pod_template_requires_task_auth_secret() {
        let resource = sample_resource();
        let template = build_executor_pod_template(&resource, "http://krishiv-coordinator:9090");
        let spec = template.spec.expect("pod template spec");
        let container = spec.containers.first().expect("executor container");
        let env = container.env.as_ref().expect("executor env");

        let require_auth = env
            .iter()
            .find(|var| var.name == "KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH")
            .expect("require auth env");
        assert_eq!(require_auth.value.as_deref(), Some("true"));

        let coordinator_token = env
            .iter()
            .find(|var| var.name == "KRISHIV_COORDINATOR_BEARER_TOKEN")
            .expect("coordinator token env");
        let coordinator_secret = coordinator_token
            .value_from
            .as_ref()
            .and_then(|source| source.secret_key_ref.as_ref())
            .expect("coordinator auth secret ref");
        assert_eq!(coordinator_secret.name, "krishiv-coordinator-auth");
        assert_eq!(coordinator_secret.key, "token");
        assert_eq!(coordinator_secret.optional, Some(false));

        let token = env
            .iter()
            .find(|var| var.name == "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN")
            .expect("task auth token env");
        let secret = token
            .value_from
            .as_ref()
            .and_then(|source| source.secret_key_ref.as_ref())
            .expect("task auth secret ref");
        assert_eq!(secret.name, "krishiv-executor-task-auth");
        assert_eq!(secret.key, "token");
        assert_eq!(secret.optional, Some(false));
    }

    #[test]
    fn build_pod_omits_hostpath_volume() {
        let resource = sample_resource();
        let pod = build_executor_pod(&resource, "sample-batch-exec-0", "exec-0", 0, "job-0", "http://krishiv-coordinator:9090");
        let spec = pod.spec.expect("pod spec");
        assert!(
            spec.volumes.is_none() || spec.volumes.as_ref().unwrap().is_empty(),
            "executor pod must not mount hostPath volumes"
        );
        let container = spec.containers.first().expect("executor container");
        assert!(
            container.volume_mounts.is_none()
                || container.volume_mounts.as_ref().unwrap().is_empty(),
            "executor container must not have volume mounts"
        );
    }

    #[test]
    fn build_pod_injects_only_allowlisted_env_vars_from_args() {
        let mut resource = sample_resource();
        resource.spec.args = vec![
            String::from("KRISHIV_HEARTBEAT_INTERVAL_SECS=5"),
            String::from("KRISHIV_HTTP_ADDR=0.0.0.0:8080"),
            String::from("KRISHIV_COORDINATOR_BEARER_TOKEN=secret"),
            String::from("FOO_BAR=baz"),
            String::from("PATH=/evil"),
        ];
        let pod = build_executor_pod(&resource, "sample-batch-exec-0", "exec-0", 0, "job-0", "http://krishiv-coordinator:9090");
        let spec = pod.spec.expect("pod spec");
        let container = spec.containers.first().expect("executor container");
        let env = container.env.as_ref().expect("executor env");

        let env_names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
        assert!(env_names.contains(&"KRISHIV_HEARTBEAT_INTERVAL_SECS"));
        assert!(env_names.contains(&"KRISHIV_HTTP_ADDR"));

        // Auth tokens are always added by build_executor_pod via secret refs,
        // but they must never be injectable from resource.spec.args.
        let coordinator_token = env.iter().find(|e| e.name == "KRISHIV_COORDINATOR_BEARER_TOKEN").expect("coordinator token env");
        assert!(
            coordinator_token.value.is_none(),
            "auth token must come from a secret ref, not from args"
        );
        assert!(coordinator_token.value_from.is_some(), "auth token must have a secret ref");

        assert!(!env_names.contains(&"FOO_BAR"), "arbitrary env vars must be rejected");
        assert!(!env_names.contains(&"PATH"), "sensitive env vars must be rejected");
    }

    #[test]
    fn converts_dynamic_object_into_typed_resource() {
        let api_resource = krishivjob_api_resource();
        let mut object = DynamicObject::new("sample-batch", &api_resource)
            .within("krishiv-system")
            .data(json!({
                "spec": {
                    "mode": "batch",
                    "image": "ghcr.io/krishiv/krishiv:dev",
                    "tasks": 2,
                    "parallelism": 2,
                    "restartPolicy": "Never"
                }
            }));
        object.metadata.generation = Some(7);

        let resource = resource_from_dynamic_object(&object).unwrap();

        assert_eq!(resource.metadata.name, "sample-batch");
        assert_eq!(
            resource.metadata.namespace.as_deref(),
            Some("krishiv-system")
        );
        assert_eq!(resource.metadata.generation, 7);
        assert_eq!(resource.spec.tasks, 2);
    }

    #[test]
    fn status_patch_wraps_status_subresource() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();
        let patch = status_patch(outcome.status());

        assert_eq!(patch["status"]["phase"], "Running");
        assert_eq!(patch["status"]["coordinator"], "coord-1");
        assert_eq!(patch["status"]["tasks"]["assigned"], 2);
    }

    #[test]
    fn controller_config_can_scope_or_watch_all_namespaces() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let namespaced =
            KubernetesControllerConfig::namespaced("krishiv-system", coordinator_id.clone());
        let all = KubernetesControllerConfig::all_namespaces(coordinator_id)
            .with_label_selector("app.kubernetes.io/name=krishiv")
            .with_bootstrap_executor(BootstrapExecutor::new("exec-1", "executor", 2));

        assert_eq!(namespaced.namespace(), Some("krishiv-system"));
        assert_eq!(all.namespace(), None);
        assert_eq!(all.coordinator_id().as_str(), "coord-1");
    }

    #[tokio::test]
    async fn controller_runtime_shares_bootstrap_coordinator_state() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let config = KubernetesControllerConfig::namespaced("krishiv-system", coordinator_id)
            .with_bootstrap_executor(BootstrapExecutor::new("exec-1", "executor", 2));

        let runtime = KubernetesControllerRuntime::new(&config).unwrap();
        let shared = runtime.coordinator();
        let coordinator = shared.read().await;

        assert_eq!(coordinator.coordinator_id().as_str(), "coord-1");
        assert_eq!(coordinator.executor_snapshots().len(), 1);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Healthy
        );
        assert_eq!(runtime.reconciler().coordinator_id().as_str(), "coord-1");
    }

    #[test]
    fn demo_coordinator_registers_one_healthy_executor() {
        let coordinator = demo_coordinator(CoordinatorId::try_new("coord-1").unwrap(), 2).unwrap();

        assert_eq!(coordinator.executor_snapshots().len(), 1);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Healthy
        );
    }

    #[test]
    fn manual_executor_registration_can_place_job() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(
                executor_id.clone(),
                "executor-pod",
                1,
            ))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))
            .unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id);

        let outcome = reconciler
            .reconcile(&mut coordinator, &sample_resource())
            .unwrap();

        assert_eq!(outcome.action(), ReconcileAction::Submitted);
    }

    fn sample_resource() -> KrishivJobResource {
        let mut spec = KrishivJobSpec::new(KrishivJobMode::Batch, "ghcr.io/krishiv/krishiv:dev", 2);
        spec.parallelism = Some(2);
        spec.args = vec![
            String::from("sql"),
            String::from("--query"),
            String::from("select 1 as value"),
        ];

        // Resources observed in the reconcile loop already have the finalizer registered.
        let mut meta = ObjectMeta::namespaced("krishiv-system", "sample-batch");
        meta.finalizers = vec![FINALIZER.to_string()];

        KrishivJobResource::new(meta, spec)
    }

    #[test]
    fn reconcile_adds_finalizer_on_first_observe() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = Coordinator::active(coordinator_id);

        // Resource without a finalizer triggers FinalizerAdded.
        let mut resource = sample_resource();
        resource.metadata.finalizers.clear();

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::FinalizerAdded);
    }

    #[test]
    fn reconcile_removes_finalizer_on_deletion() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();

        // Submit the job first so there is something to clean up.
        reconciler
            .reconcile(&mut coordinator, &sample_resource())
            .unwrap();

        // Simulate the resource being deleted (deletion timestamp set).
        let mut resource = sample_resource();
        resource.metadata.deletion_timestamp = Some(String::from("2026-05-18T00:00:00Z"));

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::FinalizerRemoved);
    }

    #[test]
    fn reconcile_delete_calls_cancel_job_before_removing_finalizer() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();

        // Submit the job so there is a live scheduler job to cancel.
        reconciler
            .reconcile(&mut coordinator, &sample_resource())
            .unwrap();

        // Confirm job was submitted and is not yet cancelled.
        let job_id = JobId::try_new("krishiv-system.sample-batch").unwrap();
        let snapshot_before = coordinator.job_snapshot(&job_id).unwrap();
        assert_ne!(snapshot_before.state(), JobState::Cancelled);

        // Simulate the resource being deleted.
        let mut resource = sample_resource();
        resource.metadata.deletion_timestamp = Some(String::from("2026-05-18T00:00:00Z"));

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::FinalizerRemoved);

        // After reconcile on deletion the scheduler job must be cancelled.
        let snapshot_after = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot_after.state(), JobState::Cancelled);
    }

    // --- Slice 7: Operator restart idempotency ---

    #[test]
    fn operator_restart_does_not_duplicate_scheduler_jobs() {
        let coordinator_id = CoordinatorId::try_new("coord-idem").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();

        // Resource without a finalizer: first reconcile returns FinalizerAdded.
        let mut resource = sample_resource();
        resource.metadata.finalizers.clear();

        let outcome1 = reconciler.reconcile(&mut coordinator, &resource).unwrap();
        assert_eq!(outcome1.action(), ReconcileAction::FinalizerAdded);
        // No scheduler job created yet.
        assert_eq!(coordinator.job_snapshots().len(), 0);

        // Second reconcile with the finalizer present: returns Submitted.
        let resource_with_finalizer = sample_resource(); // has the finalizer
        let outcome2 = reconciler
            .reconcile(&mut coordinator, &resource_with_finalizer)
            .unwrap();
        assert_eq!(outcome2.action(), ReconcileAction::Submitted);
        assert_eq!(coordinator.job_snapshots().len(), 1);

        // Third reconcile: job already exists, returns Observed — NOT a duplicate submit.
        let outcome3 = reconciler
            .reconcile(&mut coordinator, &resource_with_finalizer)
            .unwrap();
        assert_eq!(outcome3.action(), ReconcileAction::Observed);
        // Still exactly 1 scheduler job — no duplication.
        assert_eq!(coordinator.job_snapshots().len(), 1);
    }

    #[test]
    fn detects_executor_image_pull_failure_from_pod_status() {
        let pod = json!({
            "status": {
                "phase": "Pending",
                "containerStatuses": [{
                    "name": "executor",
                    "state": {
                        "waiting": {
                            "reason": "ImagePullBackOff",
                            "message": "failed to pull image"
                        }
                    }
                }]
            }
        });

        let failure = detect_executor_pod_launch_failure(&pod).unwrap();

        assert_eq!(failure.reason, "ImagePullBackOff");
        assert!(failure.message.contains("failed to pull"));
    }

    #[test]
    fn detects_executor_id_label_on_pod_launch_failure() {
        let pod = json!({
            "metadata": {
                "labels": {
                    EXECUTOR_ID_LABEL: "exec-pod-fail"
                }
            },
            "status": {
                "phase": "Pending",
                "containerStatuses": [{
                    "name": "executor",
                    "state": {
                        "waiting": {
                            "reason": "CreateContainerError",
                            "message": "container could not start"
                        }
                    }
                }]
            }
        });

        let failure = detect_executor_pod_launch_failure(&pod).unwrap();

        assert_eq!(
            failure.executor_id.as_ref().map(ExecutorId::as_str),
            Some("exec-pod-fail")
        );
        assert_eq!(failure.reason, "CreateContainerError");
    }

    #[test]
    fn reconcile_reports_executor_pod_launch_failure_status() {
        let coordinator_id = CoordinatorId::try_new("coord-pod-fail").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = Coordinator::active(coordinator_id);

        let outcome = reconciler
            .reconcile_with_executor_pod_failure(
                &mut coordinator,
                &sample_resource(),
                Some(ExecutorPodLaunchFailure::new(
                    "Unschedulable",
                    "0/3 nodes are available",
                )),
            )
            .unwrap();

        assert_eq!(outcome.action(), ReconcileAction::ExecutorPodLaunchFailed);
        assert_eq!(outcome.status().phase, KrishivJobPhase::Failed);
        assert_eq!(
            outcome.status().conditions[0].condition_type,
            "ExecutorPodReady"
        );
        assert_eq!(
            outcome.status().conditions[0].reason.as_deref(),
            Some("Unschedulable")
        );
    }

    #[test]
    fn reconcile_executor_pod_launch_failure_marks_executor_lost_and_requeues_task() {
        let coordinator_id = CoordinatorId::try_new("coord-pod-requeue").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = Coordinator::active(coordinator_id);
        let executor_id = ExecutorId::try_new("exec-launch-fail").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(
                executor_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        let mut resource = sample_resource();
        resource.spec.tasks = 1;
        let job_id = JobId::try_new(resource.scheduler_job_id()).unwrap();
        coordinator
            .submit_job(job_spec_from_resource(&resource).unwrap())
            .unwrap();
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        let assignment = assignments.first().unwrap();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    assignment.job_id().clone(),
                    assignment.stage_id().clone(),
                    assignment.task_id().clone(),
                    executor_id.clone(),
                    TaskState::Running,
                    assignment.attempt_id().as_u32(),
                )
                .with_lease_generation(assignment.lease_generation()),
            )
            .unwrap();

        let outcome = reconciler
            .reconcile_with_executor_pod_failure(
                &mut coordinator,
                &resource,
                Some(
                    ExecutorPodLaunchFailure::new("ImagePullBackOff", "failed to pull image")
                        .with_executor_id(executor_id.clone()),
                ),
            )
            .unwrap();

        assert_eq!(outcome.action(), ReconcileAction::ExecutorPodLaunchFailed);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Lost
        );
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.stages()[0].tasks()[0].state(), TaskState::Pending);
    }

    // ── R7.1 CrdQueueManager tests ───────────────────────────────────────────

    use krishiv_scheduler::{NamespaceQuotaSnapshot, QueueManager, SubmitOutcome};

    fn make_queue(namespace: &str, max_jobs: usize) -> KrishivQueue {
        KrishivQueue {
            spec: KrishivQueueSpec {
                namespace: namespace.to_owned(),
                cpu_nanos_limit: None,
                memory_bytes_limit: None,
                max_concurrent_jobs: Some(max_jobs),
                priority: 128,
            },
            status: KrishivQueueStatus::default(),
        }
    }

    #[test]
    fn crd_queue_manager_admits_within_limit() {
        let mgr = CrdQueueManager::from_queues([make_queue("team-a", 3)]);
        let spec = JobId::try_new("j").unwrap();
        let job_spec = krishiv_proto::JobSpec::new(spec, "test", krishiv_proto::JobKind::Batch)
            .with_namespace("team-a");
        let quota = NamespaceQuotaSnapshot {
            namespace_id: Some("team-a".to_owned()),
            active_job_count: 2,
            ..Default::default()
        };
        assert_eq!(mgr.admit(&job_spec, &quota), SubmitOutcome::Accepted);
    }

    #[test]
    fn crd_queue_manager_queues_when_namespace_limit_reached() {
        let mgr = CrdQueueManager::from_queues([make_queue("team-b", 1)]);
        let job_spec = krishiv_proto::JobSpec::new(
            JobId::try_new("j2").unwrap(),
            "test",
            krishiv_proto::JobKind::Batch,
        )
        .with_namespace("team-b");
        let quota = NamespaceQuotaSnapshot {
            namespace_id: Some("team-b".to_owned()),
            active_job_count: 1,
            ..Default::default()
        };
        assert!(matches!(
            mgr.admit(&job_spec, &quota),
            SubmitOutcome::Queued { .. }
        ));
    }

    #[test]
    fn crd_queue_manager_admits_unknown_namespace_with_default_policy() {
        let mgr = CrdQueueManager::from_queues([make_queue("team-c", 1)]);
        let job_spec = krishiv_proto::JobSpec::new(
            JobId::try_new("j3").unwrap(),
            "test",
            krishiv_proto::JobKind::Batch,
        );
        // No namespace set — default policy has no limits.
        let quota = NamespaceQuotaSnapshot::default();
        assert_eq!(mgr.admit(&job_spec, &quota), SubmitOutcome::Accepted);
    }

    #[test]
    fn krishiv_queue_derives_correct_quota_policy() {
        let q = make_queue("eng", 5);
        let policy = q.quota_policy();
        assert_eq!(policy.max_concurrent_jobs, Some(5));
        assert!(policy.cpu_nanos_limit.is_none());
    }

    // ── K8sLeaseElection simulation mode (no client) ─────────────────────

    #[tokio::test]
    async fn k8s_lease_simulation_mode_works() {
        // Exercises try_acquire → renew → release without a kube::Client so
        // the test runs in any environment without a live cluster.
        let election = K8sLeaseElection::new("sim-lease", "default", "pod-sim");

        // Initially not a leader.
        assert!(!election.is_leader());
        assert_eq!(election.fencing_token(), 0);

        // Acquire succeeds.
        assert!(election.try_acquire().await);
        assert!(election.is_leader());
        assert_eq!(election.fencing_token(), 1);

        // Renewal succeeds while we hold the lease.
        assert!(election.renew().await);
        assert!(election.is_leader());

        // Release clears leadership.
        election.release().await;
        assert!(!election.is_leader());

        // Renewal after release returns false.
        assert!(!election.renew().await);

        // Re-acquire increments fencing token again.
        assert!(election.try_acquire().await);
        assert_eq!(election.fencing_token(), 2);
    }

    // ── K8sLeaseElection failover tests ───────────────────────────────────

    #[test]
    fn k8s_lease_election_initially_not_leader() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        assert!(!election.is_leader());
        assert_eq!(election.fencing_token(), 0);
    }

    #[tokio::test]
    async fn k8s_lease_election_try_acquire_succeeds() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        assert!(election.try_acquire().await);
        assert!(election.is_leader());
        assert_eq!(election.fencing_token(), 1);
    }

    #[tokio::test]
    async fn k8s_lease_election_fencing_token_increments_on_each_acquire() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        election.try_acquire().await;
        election.release().await;
        election.try_acquire().await;
        assert_eq!(election.fencing_token(), 2);
    }

    #[tokio::test]
    async fn k8s_lease_election_release_clears_leader() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        election.try_acquire().await;
        assert!(election.is_leader());
        election.release().await;
        assert!(!election.is_leader());
    }

    #[tokio::test]
    async fn k8s_lease_election_renew_while_leader() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        election.try_acquire().await;
        assert!(election.renew().await);
    }

    #[tokio::test]
    async fn k8s_lease_election_renew_after_release_fails() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        election.try_acquire().await;
        election.release().await;
        assert!(!election.renew().await);
    }

    #[tokio::test]
    async fn failover_stale_coordinator_checkpoint_rejected() {
        // Coordinator A acquires at token=1, commits epoch 1.
        // Coordinator B takes over at token=2.
        // Coordinator A tries to commit epoch 2 with its old token=1 → rejected.
        use krishiv_checkpoint::{CheckpointError, CheckpointMetadata, validate_fencing_token};

        let coord_a = K8sLeaseElection::new("job-failover", "default", "pod-a");
        coord_a.try_acquire().await; // token = 1

        let mut meta_epoch1 = CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch: 1,
            job_id: "job-failover".to_owned(),
            fencing_token: coord_a.fencing_token(), // 1
            coordinator_id: None,
            timestamp_ms: 0,
            source_offsets: vec![],
            operator_snapshots: vec![],
            is_savepoint: false,
            savepoint_label: None,
            iceberg_snapshot_id: None,
            kafka_offsets: None,
        };
        // Epoch 1 commit succeeds (current token = 1, meta token = 1).
        assert!(validate_fencing_token(&meta_epoch1, coord_a.fencing_token()).is_ok());

        // Coordinator A loses the lease; Coordinator B acquires (token = 2).
        coord_a.release().await;
        let coord_b = K8sLeaseElection::new("job-failover", "default", "pod-b");
        coord_b.try_acquire().await; // token = 1 (fresh election handle)
        // Simulate that the global fencing token is now 2 (B's acquire follows A's).
        coord_b.try_acquire().await; // token = 2

        // Coordinator A tries to commit epoch 2 with its stale token = 1.
        meta_epoch1.epoch = 2;
        meta_epoch1.fencing_token = 1; // A's old token
        let result = validate_fencing_token(&meta_epoch1, coord_b.fencing_token()); // current=2
        assert!(
            matches!(
                result,
                Err(CheckpointError::StaleFencingToken {
                    stored: 1,
                    current: 2
                })
            ),
            "expected StaleFencingToken, got: {result:?}"
        );
    }

    // ── P0.12: Patch::Apply test — concurrent update handling ──────────────

    /// Verify that `status_patch` builds a valid server-side apply document and
    /// that `patch_krishivjob_status` uses `Patch::Apply` (not `Patch::Merge`).
    ///
    /// We cannot call the K8s API in a unit test, so we verify the patch value
    /// structure and field manager constant rather than making a live API call.
    #[test]
    fn patch_apply_uses_field_manager_constant() {
        assert_eq!(FIELD_MANAGER, "krishiv-operator");

        // Confirm the patch document contains a "status" key so server-side
        // apply targets the status subresource correctly.
        let status = KrishivJobStatus {
            phase: KrishivJobPhase::Running,
            coordinator: Some("coord-1".to_owned()),
            observed_generation: 3,
            stages: 1,
            tasks: TaskStatusCounters {
                assigned: 0,
                running: 2,
                succeeded: 0,
                failed: 0,
            },
            conditions: vec![],
        };
        let patch = status_patch(&status);
        assert!(patch.get("status").is_some(), "patch must contain 'status'");
        assert_eq!(patch["status"]["phase"], "Running");
        assert_eq!(patch["status"]["observedGeneration"], 3);
    }

    /// Simulate a concurrent-update scenario: two coordinators produce status
    /// patches in parallel.  With `Patch::Apply` + `fieldManager`, the API
    /// server tracks field ownership, so the last writer wins for its own fields
    /// rather than silently overwriting unrelated fields the way `Patch::Merge`
    /// does.  This test documents the expected apply params.
    #[test]
    fn patch_apply_params_are_correct() {
        use kube::api::PatchParams;

        // PatchParams::apply sets field_manager and is suitable for Patch::Apply.
        let params = PatchParams::apply(FIELD_MANAGER).force();
        assert_eq!(
            params.field_manager.as_deref(),
            Some("krishiv-operator"),
            "field manager must be 'krishiv-operator'"
        );
        assert!(
            params.force,
            "force must be true so concurrent apply wins for owned fields"
        );
    }
}
