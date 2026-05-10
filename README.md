# Airflow on KIND: GitHub DAG Sync to S3

MLOps data collection reference by **Rajinikanth Vadla**.

## Architecture

![Airflow on KIND MLOps architecture](docs/airflow-mlops-kubernetes.svg)

Editable Draw.io source: [`docs/airflow-mlops-kubernetes.drawio`](docs/airflow-mlops-kubernetes.drawio)

## What Runs

- KIND cluster: **3 nodes** (`1 control-plane`, `2 workers`) from `kind-config.yaml`.
- Airflow: installed by Helm chart from `helm/airflow-values.yaml`.
- DAG sync: Helm `dags.gitSync` pulls the GitHub `dags/` folder every `60` seconds.
- DAG files:
  - `dags/api_user_collection.py` → `api_user_collection_to_s3`
  - `dags/scraping_collection.py` → `scraping_collection_to_s3`
  - `dags/crawling_collection.py` → `crawling_collection_to_s3`
  - `dags/google_public_api_collection.py` → `google_public_api_collection_to_s3`
- Final output: CSV files uploaded to **AWS S3**.

## Data Flow

1. Push DAG code to GitHub.
2. Airflow `git-sync` pulls all Python DAGs from `dags/` into Kubernetes.
3. Scheduler parses the four collection DAGs and queues tasks.
4. Celery workers collect data from user API, website scraping, website crawling, and Google Books API.
5. Worker writes CSV files in `/opt/airflow/data`.
6. Each DAG uploads its own CSV file to `s3://S3_BUCKET/S3_PREFIX/<source-folder>/`.

## Prerequisites

- Docker
- kind
- kubectl
- Helm 3
- AWS access key, secret key, region, and S3 bucket
- GitHub HTTPS repo URL for this repository

## 1. Check KIND Nodes

`kind-config.yaml` already creates 3 nodes:

```yaml
nodes:
  - role: control-plane
  - role: worker
  - role: worker
```

No `extraMounts` are used.

## 2. Create Cluster

```powershell
kind create cluster --name airflow-lab --config kind-config.yaml
```

## 3. Build Airflow Image

```powershell
docker build -t airflow-lab:local .
kind load docker-image airflow-lab:local --name airflow-lab
```

## 4. Create Namespace and Helm Repo

```powershell
helm repo add apache-airflow https://airflow.apache.org/charts
helm repo update
kubectl create namespace airflow
```

## 5. Create S3 Secret

```powershell
kubectl create secret generic aws-s3-creds -n airflow `
  --from-literal=AWS_ACCESS_KEY_ID=YOUR_KEY `
  --from-literal=AWS_SECRET_ACCESS_KEY=YOUR_SECRET `
  --from-literal=AWS_DEFAULT_REGION=us-east-1 `
  --from-literal=S3_BUCKET=YOUR_BUCKET `
  --from-literal=S3_PREFIX=airflow-collected
```

The DAG upload task fails if `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or `S3_BUCKET` is missing.

## 6. Set GitHub Repo URL

Open `helm/airflow-values.yaml` and replace:

```yaml
repo: https://github.com/YOUR_ORG/airflow-labs-data-collection-quickstart.git
```

Use the HTTPS URL of the GitHub repo that contains this `dags/` folder.

## 7. Install Airflow

```powershell
helm upgrade --install airflow apache-airflow/airflow -n airflow `
  -f helm/airflow-values.yaml `
  -f helm/workers-s3-secret.yaml `
  --version 1.18.0
```

## 8. Check Pods

```powershell
kubectl get pods -n airflow
```

Wait for Airflow pods to become `Running` and setup jobs to become `Completed`.

## 9. Open Airflow UI

```powershell
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
```

Open:

```text
http://localhost:8080
```

Login:

```text
admin / admin
```

## 10. Run DAG

Run any DAG:

- `api_user_collection_to_s3`
- `scraping_collection_to_s3`
- `crawling_collection_to_s3`
- `google_public_api_collection_to_s3`

Steps:

1. Open one DAG.
2. Unpause it.
3. Click **Trigger DAG**.
4. Verify CSV files in S3 under:

```text
s3://YOUR_BUCKET/airflow-collected/<source-folder>/
```

## 11. Debug CSV Files Inside Worker Pod

Use only if you want to inspect local staging files before S3 upload:

```powershell
$worker = kubectl get pod -n airflow -l component=worker -o jsonpath="{.items[0].metadata.name}"
kubectl exec -n airflow "$worker" -c worker -- ls -l /opt/airflow/data
```

## 12. Update DAGs from GitHub

1. Edit files under `dags/`.
2. Push to GitHub branch `main`.
3. Wait `60` seconds for git-sync.
4. Airflow loads the updated DAG code.

## Cleanup

```powershell
kind delete cluster --name airflow-lab
```

---

Repository tag: **Rajinikanth Vadla**  MLOps data collection reference.
