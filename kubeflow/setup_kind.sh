#!/usr/bin/env bash
# Kubeflow Pipelines standalone sur un cluster kind local (necessite docker, kind, kubectl)
# A lancer APRES `docker compose up -d` : le cluster est branche sur le reseau compose pour atteindre HDFS/MLflow/MinIO.
set -e
PIPELINE_VERSION=2.2.0
CLUSTER=fraud
kind create cluster --name "$CLUSTER"
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=$PIPELINE_VERSION"
kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=$PIPELINE_VERSION"

# Connexion HDFS <-> Kubeflow : le streaming ecrit dans HDFS (namenode/datanode du docker compose). On branche le noeud kind
# sur le reseau compose : les pods resolvent alors namenode, mlflow et minio, et joignent le datanode (necessaire pour lire/ecrire).
NET=$(docker inspect namenode --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null || true)
if [ -z "$NET" ]; then
  echo "ATTENTION : conteneur 'namenode' introuvable. Lance d'abord : docker compose up -d"; exit 1
fi
docker network connect "$NET" "${CLUSTER}-control-plane" 2>/dev/null || echo "(noeud kind deja connecte a $NET)"
echo "Noeud kind connecte au reseau $NET"

# Images : entrainement (Kubeflow) et API de prediction (staging / preprod)
docker build -f docker/Dockerfile.train -t fraud-train:v1 .
kind load docker-image fraud-train:v1 --name "$CLUSTER"
echo "UI : kubectl port-forward -n kubeflow svc/ml-pipeline-ui 8081:80  -> http://localhost:8081"
echo "Puis importer kubeflow/fraud_pipeline.yaml dans l'UI (parametres par defaut = HDFS du docker compose)."
