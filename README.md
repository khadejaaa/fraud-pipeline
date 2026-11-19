# Detection de fraude bancaire - pipeline Big Data & ML

```
fraud_gen.py (Python) -> Kafka -> Spark Streaming -> HDFS (stockage distribue)
                                        |                 |
                                        |                 +-> Spark Batch (nettoyage, agregations) -> HDFS /lake/gold
                                        +-> Console (agregations temps reel)
HDFS -> Spark ML -> MLflow (suivi + registre) -> API de prediction (FastAPI)
Airflow / Kubeflow : orchestration  |  Rapport HTML exporte dans reports/
```

## Lancement
1. Ouvrir Docker Desktop, puis : `docker compose up -d --build` (le 1er lancement est long).
2. `docker compose ps` : tous les services en `running` (`minio-init` finit en `exited (0)`, normal).
3. Generateur (sur ta machine, dans un venv) :
   `pip install -r producer/requirements.txt && python producer/fraud_gen.py`
4. **Consumer Kafka** (verifier que les messages arrivent), dans un autre terminal :
   `python consumer/kafka_consumer.py --max 20`
5. **Streaming en direct** (agregations par client et par marchand, triees) :
   `docker logs -f spark-streaming`
6. **HDFS** : laisser tourner 5 a 10 min, puis verifier :
   - interface : http://localhost:9870 -> Utilities -> Browse the file system -> `/lake/transactions`
   - ou : `docker exec namenode hdfs dfs -ls -R /lake/transactions`
7. **Batch + entrainement + rapport** : Airflow http://localhost:8085 (mot de passe `admin` dans `docker logs airflow`),
   activer et lancer le DAG `fraud_retrain` (check_hdfs -> batch_job -> train_and_log -> make_report).
   Ou a la main :
   ```
   docker exec airflow python /opt/project/batch/batch_job.py
   docker exec airflow python /opt/project/spark/train.py
   docker exec airflow python /opt/project/report/make_report.py
   ```
8. **Rapport exporte** : `reports/rapport_fraude.html` (ouvrir dans le navigateur ; Imprimer -> PDF).
   Il est aussi **stocke dans HDFS** : `docker exec namenode hdfs dfs -ls /lake/reports` ou http://localhost:9870 -> `/lake/reports`.
9. **MLflow** : http://localhost:5000 -> experiment `fraud-detection` (comparer logreg / random_forest) et onglet *Models* (`fraud-detector`, alias `champion`).
10. **Deploiement du modele** (apres l'etape 7) :
    ```
    curl http://localhost:8000/health
    curl -X POST http://localhost:8000/predict -H "Content-Type: application/json" ^
      -d "{\"amount\":1200,\"merchant\":\"luxury\",\"country\":\"CN\",\"home_country\":\"MA\",\"hour\":3,\"device_new\":true}"
    ```
    (PowerShell : utiliser `curl.exe`, ou l'interface http://localhost:8000/docs).
    Si l'API repond 503 : l'entrainement n'a pas encore enregistre de modele. Apres un nouvel entrainement : `POST /reload`.

## Correspondance avec le cahier des charges
| Exigence du prof | Ou dans le projet |
|---|---|
| Generateur de donnees (boucle continue) | `producer/fraud_gen.py` |
| Kafka : broker, topic, producer, consumer | `docker-compose.yml` (kafka), `producer/`, `consumer/kafka_consumer.py` |
| Streaming : lecture Kafka, preparation, agregations par client, tri | `spark/streaming_job.py` |
| Sorties multiples : HDFS + console | `streaming_job.py` (parquet HDFS + 2 sorties console) |
| Stockage HDFS, repertoires, verification | services `namenode`, `datanode` ; UI :9870 |
| Traitement batch (nettoyage, agregations, stats) | `batch/batch_job.py` -> `/lake/gold` + `reports/batch_summary.json` |
| Machine Learning (modele, entrainement, predictions, metriques) | `spark/train.py` (logreg, random forest, precision/rappel/F1/AUC-PR) |
| Rapport personnalise exporte (et stocke dans HDFS) | `report/make_report.py` -> `reports/rapport_fraude.html` + HDFS `/lake/reports` |
| Deploiement (Docker Compose, modele servi) | `docker-compose.yml`, `serving/app.py` (service `fraud-api`) |
| Orchestration (schema du prof : Airflow, Kubeflow) | `airflow/dags/fraud_retrain.py`, `kubeflow/` |
| Jupyter : construction du modele (Analyst) | service `jupyter` (http://localhost:8888/?token=fraud), `notebooks/model_building.ipynb` |
| Log du modele dans le registre MLflow | `spark/train.py`, notebook |
| Assign model stage (Senior Analyst) | `scripts/promote_model.py` : `request` (Analyst) -> `approve` / `reject` (Senior Analyst, validation humaine) |
| Prepare deployments, k8s staging et pre prod | `k8s/base`, `k8s/overlays/staging`, `k8s/overlays/preprod` |
| Git -> Argo CD (sync deployments) | `argocd/application-*.yaml` (depot a renseigner : `scripts/configure_git.py`) |

> L'image du prof ne contient **pas** Octopus Deploy : la pieuvre a cote de "Sync deployments" est le logo d'**Argo CD**. Aucun outil supplementaire n'est necessaire.

MinIO ne sert plus que de stockage des artefacts MLflow ; les donnees du pipeline sont dans HDFS.

## Deploiement Kubernetes : staging et pre prod (partie droite du schema)
Necessite kind, kubectl, et le projet pousse sur ton depot Git. Les services docker compose doivent tourner.
1. Cluster : `kind create cluster --name fraud` (ou `./kubeflow/setup_kind.sh`, qui le cree deja)
2. Image de l'API dans kind : `docker compose build fraud-api` puis `kind load docker-image fraud-pipeline-fraud-api:latest --name fraud`
3. **Validation par le Senior Analyst**, apres un entrainement (l'alias n'est jamais pose sans approbation) :
   ```
   docker exec airflow python /opt/project/scripts/promote_model.py status
   docker exec airflow python /opt/project/scripts/promote_model.py request --alias staging --requester analyste1
   docker exec -it airflow python /opt/project/scripts/promote_model.py approve --alias staging --approver senior1
   ```
   `approve` demande de taper `APPROUVER`, refuse l'auto-approbation (approbateur = demandeur) et, pour `preprod`, exige que
   la version ait deja ete approuvee pour `staging`. `reject --comment "..."` refuse une demande.
   Lance `approve` **depuis le dossier du projet** (`python scripts/promote_model.py approve ...`, avec `MLFLOW_TRACKING_URI=http://localhost:5000`)
   pour qu'il ecrive aussi `MODEL_VERSION` dans `k8s/overlays/<env>/kustomization.yaml` ("Prepare deployments"), puis :
   `git add k8s && git commit -m "staging: modele vN" && git push`. Le ConfigMap change, Argo CD redeploie les pods, ils rechargent le modele.
4. Depot Git : `python scripts/configure_git.py --repo-url https://github.com/<compte>/<depot>.git`, puis pousse le **contenu** de ce dossier
   a la racine du depot (Argo CD lit `k8s/overlays/<env>` a la racine).
5. Sans Argo CD (test rapide) : `kubectl apply -k k8s/overlays/staging` puis `kubectl apply -k k8s/overlays/preprod`
6. Avec Argo CD : installer Argo CD, puis `kubectl apply -f argocd/application-staging.yaml -f argocd/application-preprod.yaml`
7. Tester : `kubectl -n staging get pods`, `kubectl -n staging port-forward svc/fraud-api 8100:80` puis http://localhost:8100/docs
Les pods joignent MLflow et MinIO via `host.docker.internal` (a adapter si ton reseau est different). Les identifiants MinIO sont dans un
Secret genere par chaque overlay (valeurs de DEV : en production, utiliser SealedSecrets ou External Secrets).

## Kubeflow (partie du schema)
Necessite docker, kind et kubectl. Pipeline `kubeflow/pipeline.py` : `check_hdfs -> prepare_data -> train_and_register -> verify_registry`.
Il lit les donnees du streaming **dans HDFS** (`hdfs://namenode:9000/lake/transactions/`), et plus dans `s3a://lake` (bucket MinIO vide).
1. `docker compose up -d` (HDFS, MLflow et MinIO doivent tourner), puis `pip install kfp && python kubeflow/pipeline.py` -> `kubeflow/fraud_pipeline.yaml`
2. `./kubeflow/setup_kind.sh` : cluster kind + Kubeflow Pipelines + **branchement du noeud kind sur le reseau docker compose**
   (les pods resolvent `namenode`, `mlflow`, `minio`) + image `fraud-train:v1`.
3. `kubectl port-forward -n kubeflow svc/ml-pipeline-ui 8081:80`, ouvre http://localhost:8081, importe le YAML et lance un run (parametres par defaut OK).
4. Le modele entraine arrive dans MLflow (alias `champion`) : suite = validation du Senior Analyst (section precedente).

## Tests realises (sans Docker)
```
pip install pyspark==3.5.1 mlflow==2.14.1 pandas pyarrow fastapi httpx kfp
python tests/make_sample_data.py /tmp/sample 6000
python spark/train.py --data-path /tmp/sample --mlflow-uri file:/tmp/mlruns --reports-dir /tmp/rep
python tests/test_promotion_workflow.py file:/tmp/mlruns     # circuit d'approbation (14 tests)
python tests/test_kubeflow_steps.py /tmp/sample file:/tmp/mlruns   # scripts des etapes Kubeflow (8 tests)
```
**Valide** : entrainement + enregistrement MLflow, circuit request/approve/reject, etapes Kubeflow en local (faux WebHDFS), compilation du pipeline,
`kustomize build` + `kubeconform` des deux overlays, changement de ConfigMap a chaque nouvelle version, API de prediction chargee via l'alias.

**NON teste (necessite Docker/kind/Argo CD sur ta machine)** : docker compose complet, lecture HDFS depuis un pod Kubeflow, execution reelle du pipeline,
synchronisation Argo CD, pods staging/preprod en `Running`, joignabilite de `host.docker.internal` depuis kind.

## Metriques
Precision, rappel, F1, AUC-PR (l'accuracy est trompeuse : ~2% de fraudes).
