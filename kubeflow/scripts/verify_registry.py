"""Derniere etape du pipeline Kubeflow : controle que le modele est bien enregistre dans le registre MLflow.
  python verify_registry.py --mlflow-uri http://mlflow:5000 --model-name fraud-detector
Sortie 0 si l'alias 'champion' existe, avec ses metriques ; sinon sortie 1.
"""
import argparse, sys
from mlflow import MlflowClient

ap = argparse.ArgumentParser()
ap.add_argument("--mlflow-uri", required=True)
ap.add_argument("--model-name", default="fraud-detector")
a = ap.parse_args()

c = MlflowClient(tracking_uri=a.mlflow_uri)
try:
    mv = c.get_model_version_by_alias(a.model_name, "champion")
except Exception as e:
    sys.exit(f"ECHEC : pas de version 'champion' pour '{a.model_name}' dans {a.mlflow_uri} ({e})")
m = {k: round(v, 3) for k, v in c.get_run(mv.run_id).data.metrics.items()}
print(f"OK : '{a.model_name}' version {mv.version} (alias champion) | metriques : {m}")
print("Prochaine etape : approbation par le Senior Analyst (scripts/promote_model.py request / approve).")
