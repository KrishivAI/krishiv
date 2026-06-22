#!/usr/bin/env bash
# Redeploy the coordinator to K8s with the new image.
# Run after docker build completes.
set -euo pipefail

echo "=== Step 1: Import image to k3s ==="
docker save localhost/krishiv:local | k3s ctr images import /dev/stdin
echo "Image imported."

echo "=== Step 2: Patch coordinator deployment ==="
# Force a rollout restart to pick up the new image
kubectl -n krishiv-system rollout restart deployment/coordinator

echo "=== Step 3: Wait for rollout ==="
kubectl -n krishiv-system rollout status deployment/coordinator --timeout=120s

echo "=== Step 4: Verify new pod is running ==="
kubectl -n krishiv-system get pods -l app=krishiv-coordinator

echo "=== Step 5: Check coordinator started ==="
sleep 5
kubectl -n krishiv-system logs -l app=krishiv-coordinator --tail=10
