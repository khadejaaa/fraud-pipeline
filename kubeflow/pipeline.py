"""Pipeline Kubeflow (KFP v2) : memes etapes que le DAG Airflow, lues depuis HDFS (sortie du streaming).

  check_hdfs -> prepare_data -> train_and_register -> verify_registry

  1. check_hdfs          : les fichiers Parquet du streaming existent dans HDFS
  2. prepare_data        : nettoyage + agregations (batch_job.py) -> HDFS /lake/gold
  3. train_and_register  : Spark ML, metriques et modele logges dans MLflow, alias 'champion' (train.py)
  4. verify_registry     : le modele est bien dans le registre MLflow
Ensuite : validation par le Senior Analyst (scripts/promote_model.py) -> staging -> preprod.

Compiler : python kubeflow/pipeline.py  -> kubeflow/fraud_pipeline.yaml
Les services (namenode, mlflow, minio) sont atteints par leur nom docker compose : le cluster kind doit etre
connecte au reseau compose (fait par kubeflow/setup_kind.sh).
"""
from kfp import dsl, compiler

# Tag fixe (pas :latest) : avec :latest Kubernetes force un pull depuis un registre, et l'image chargee dans kind est ignoree.
IMAGE = "fraud-train:v1"


@dsl.container_component
def check_hdfs(data_path: str):
    return dsl.ContainerSpec(image=IMAGE, command=["python", "/app/check_hdfs.py"],
                             args=["--data-path", data_path])


@dsl.container_component
def prepare_data(data_path: str, gold_path: str):
    return dsl.ContainerSpec(image=IMAGE, command=["python", "/app/batch_job.py"],
                             args=["--data-path", data_path, "--gold-path", gold_path])


@dsl.container_component
def train_and_register(minio_endpoint: str, mlflow_uri: str, data_path: str):
    return dsl.ContainerSpec(image=IMAGE, command=["python", "/app/train.py"],
                             args=["--minio-endpoint", minio_endpoint, "--mlflow-uri", mlflow_uri,
                                   "--data-path", data_path])


@dsl.container_component
def verify_registry(mlflow_uri: str):
    return dsl.ContainerSpec(image=IMAGE, command=["python", "/app/verify_registry.py"],
                             args=["--mlflow-uri", mlflow_uri])


@dsl.pipeline(name="fraud-retrain", description="Reentrainement du modele de fraude depuis HDFS")
def fraud_pipeline(
    # Donnees ecrites par le streaming : HDFS (et non plus s3a://lake, bucket MinIO qui reste vide)
    data_path: str = "hdfs://namenode:9000/lake/transactions/",
    gold_path: str = "hdfs://namenode:9000/lake/gold/",
    mlflow_uri: str = "http://mlflow:5000",
    minio_endpoint: str = "http://minio:9000",   # artefacts MLflow
):
    check = check_hdfs(data_path=data_path)
    prep = prepare_data(data_path=data_path, gold_path=gold_path).after(check)
    train = train_and_register(minio_endpoint=minio_endpoint, mlflow_uri=mlflow_uri,
                               data_path=data_path).after(prep)
    verify_registry(mlflow_uri=mlflow_uri).after(train)


if __name__ == "__main__":
    compiler.Compiler().compile(fraud_pipeline, "kubeflow/fraud_pipeline.yaml")
    print("OK -> kubeflow/fraud_pipeline.yaml")
