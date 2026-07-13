# CI/CD for Machine Learning Models on Microsoft Fabric using `fabric-cicd` + Azure DevOps

This guide explains how to use the [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/) Python library, together with **Azure Repos** as the source of truth and **Azure Pipelines** as the automation engine, to build an end-to-end CI/CD pipeline for a **machine learning model** that lives in a Microsoft Fabric workspace.

It covers the conceptual model, repo layout, authentication via workload identity federation, the actual YAML pipelines, parameterization across DEV/PPE/PROD, and ML-specific concerns like model registration, promotion, and inferencing pipelines.

---

## 1. What `fabric-cicd` is (and isn't)

`fabric-cicd` is a Python library, maintained by Microsoft, that talks to the Fabric REST APIs so you don't have to. It deploys **source-controlled Fabric items** from a folder on disk into a target Fabric workspace.

**Key behaviors:**

- **Full deployment, every time** — it does not look at git diffs; the repo is the desired state.
- Deploys into the **tenant of the executing identity** (the credential used at runtime).
- Only supports Fabric items that have **Source Control + public Create/Update APIs**.
- Performs **parameter replacement** so the same code can target multiple environments.

**ML-relevant supported item types** (full list in the docs):

| Item | Role in an ML workflow |
|---|---|
| `Notebook` | Training, feature engineering, evaluation, batch scoring code |
| `MLExperiment` | The Fabric MLflow experiment that tracks runs/metrics |
| `Environment` | Spark + Python library environment used by notebooks |
| `Lakehouse` | Feature tables, training datasets, prediction outputs |
| `DataPipeline` | Orchestrates training / scoring notebooks on a schedule |
| `SparkJobDefinition` | Alternative to notebooks for heavier training jobs |
| `VariableLibrary` | Holds environment-scoped values (model name, alias, thresholds) |
| `Reflex` / `DataActivator` | Trigger retraining when drift or metrics breach thresholds |

> ⚠️ **Not** supported as a deployable item: **ML Models** themselves. A "Fabric ML Model" is a registered artifact produced by a training run, not a source-controlled item. `fabric-cicd` deploys the *code that produces and consumes* the model; the model lifecycle (register, version, alias/promote) is handled inside notebooks via **MLflow**.

---

## 2. Reference architecture

```
┌─────────────────────┐       ┌──────────────────────┐      ┌──────────────────────┐
│  DEV Fabric WS      │       │  PPE / TEST Fabric WS│      │  PROD Fabric WS      │
│  (git-connected)    │       │  (deploy target)     │      │  (deploy target)     │
│                     │       │                      │      │                      │
│  Notebooks          │       │  Notebooks           │      │  Notebooks           │
│  MLExperiment       │       │  MLExperiment        │      │  MLExperiment        │
│  Environment        │       │  Environment         │      │  Environment         │
│  Lakehouse(s)       │       │  Lakehouse(s)        │      │  Lakehouse(s)        │
│  Pipelines          │       │  Pipelines           │      │  Pipelines           │
└──────────┬──────────┘       └──────────▲───────────┘      └──────────▲───────────┘
           │ git sync                    │ fabric-cicd                  │ fabric-cicd
           ▼                             │ publish_all_items            │ publish_all_items
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Azure DevOps (Azure Repos)                             │
│   workspace/   parameter.yml   pipelines/{ci.yml, cd-ppe.yml, cd-prod.yml}      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

- **DEV workspace** is the only workspace **git-connected** through the Fabric UI directly to Azure Repos. Data scientists develop there; commits flow back to the repo automatically.
- **PPE** and **PROD** workspaces are **deployed to** via `fabric-cicd` from Azure Pipelines. They are never directly edited.
- Promotion of a trained **model** between workspaces happens *inside* a notebook job (MLflow `register_model` + alias), not by `fabric-cicd`.

> Fabric's native Git integration supports Azure DevOps out of the box — use **Workspace settings → Git integration → Azure DevOps**, choose org / project / repo / branch / folder (e.g. `workspace`), and connect.

---

## 3. Repository layout

```
repo-root/
├── workspace/                          # Mirrors the Fabric workspace folder
│   ├── train_model.Notebook/
│   │   ├── notebook-content.py
│   │   └── .platform
│   ├── batch_score.Notebook/
│   ├── evaluate_model.Notebook/
│   ├── churn_experiment.MLExperiment/
│   ├── ml_runtime.Environment/
│   │   ├── Setting/
│   │   └── Libraries/
│   ├── feature_store.Lakehouse/
│   ├── train_and_register.DataPipeline/
│   ├── nightly_scoring.DataPipeline/
│   └── ml_vars.VariableLibrary/
├── parameter.yml                       # MUST live at repository_directory root
├── deploy/
│   └── deploy.py                       # Thin wrapper around fabric-cicd
├── tests/
│   └── test_training.py                # Unit tests for helper modules
└── pipelines/
    ├── ci.yml
    ├── cd-ppe.yml
    └── cd-prod.yml
```

The Fabric DEV workspace is **git-connected** to the `workspace/` subfolder on a `main`-tracking branch (or a feature-branch workflow if you prefer).

---

## 4. Authentication from Azure Pipelines to Fabric

Use a **Microsoft Entra service principal** (or a User-Assigned Managed Identity) — **never** a username/password.

**Recommended: Workload Identity Federation via an ARM service connection** (no secrets stored in Azure DevOps):

1. In Azure DevOps **Project settings → Service connections → New → Azure Resource Manager → Workload identity federation (automatic)**. Create one connection per environment, e.g. `fabric-ppe-sc` and `fabric-prod-sc`.
2. The wizard creates (or reuses) an Entra app registration and adds the federated credential pointing at your DevOps project/service connection.
3. Grant that SP **Workspace Admin** (or at minimum **Member** + **Contributor** on every item type) on the **target** Fabric workspace(s).
4. Enable the tenant setting **"Service principals can use Fabric APIs"** for that SP's security group.
5. In Azure DevOps create **Environments** named `ppe` and `prod` (Pipelines → Environments). Add **Approvals and checks** on `prod`.

Useful variable groups (Pipelines → Library):

| Variable group | Variables |
|---|---|
| `fabric-common` | `REPO_DIR=workspace` |
| `fabric-ppe` | `FABRIC_WORKSPACE_ID`, `AZURE_SUBSCRIPTION=fabric-ppe-sc` |
| `fabric-prod` | `FABRIC_WORKSPACE_ID`, `AZURE_SUBSCRIPTION=fabric-prod-sc` |

Inside Python, use `AzureCliCredential` after `AzureCLI@2` has logged in via the service connection — workload identity federation is transparent to the credential.

---

## 5. The deploy script (`deploy/deploy.py`)

```python
import argparse
from azure.identity import AzureCliCredential
from fabric_cicd import (
    FabricWorkspace,
    publish_all_items,
    unpublish_all_orphan_items,
)

ML_ITEM_TYPES = [
    "Environment",
    "Lakehouse",
    "VariableLibrary",
    "MLExperiment",
    "Notebook",
    "DataPipeline",
    "Reflex",
]

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", required=True, choices=["PPE", "PROD"])
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--repo-dir", default="workspace")
    parser.add_argument("--unpublish-orphans", action="store_true")
    args = parser.parse_args()

    credential = AzureCliCredential()

    target = FabricWorkspace(
        workspace_id=args.workspace_id,
        environment=args.environment,
        repository_directory=args.repo_dir,
        item_type_in_scope=ML_ITEM_TYPES,
        token_credential=credential,
    )

    publish_all_items(target)

    if args.unpublish_orphans:
        unpublish_all_orphan_items(target)

if __name__ == "__main__":
    main()
```

Notes:
- All `FabricWorkspace` args **must be keyword args** (library requirement).
- `environment` must match a key used in `parameter.yml`.
- Run `unpublish_all_orphan_items` only in PROD if you truly want the repo to be the absolute source of truth — turn it off when multiple teams share a workspace.

---

## 6. `parameter.yml` — making one repo serve many environments

```yaml
find_replace:
  # Lakehouse id used by training/scoring notebooks
  - find_value: "dev-feature-lakehouse-guid"
    replace_value:
      PPE:  "ppe-feature-lakehouse-guid"
      PROD: "prod-feature-lakehouse-guid"
    item_type: ["Notebook", "DataPipeline"]

  # MLflow experiment name baked into notebook code
  - find_value: "/experiments/churn-dev"
    replace_value:
      PPE:  "/experiments/churn-ppe"
      PROD: "/experiments/churn-prod"
    item_type: "Notebook"

key_value_replace:
  # VariableLibrary values
  - find_key: $.variables[?(@.name=="model_alias")].value
    replace_value:
      PPE:  "challenger"
      PROD: "champion"

  - find_key: $.variables[?(@.name=="drift_threshold")].value
    replace_value:
      PPE:  "0.10"
      PROD: "0.05"

spark_pool:
  - instance_pool_id: "dev-pool-instance-id"
    replace_value:
      PPE:
        type: "Capacity"
        name: "PPE-ML-Pool"
      PROD:
        type: "Capacity"
        name: "PROD-ML-Pool"
```

The pattern: **commit DEV values into source** (the values the git-connected workspace actually uses), then *rewrite at deploy time* for PPE/PROD.

---

## 7. Azure Pipelines

### 7.1 `pipelines/ci.yml` — runs on every PR

```yaml
trigger: none

pr:
  branches:
    include: [main]

pool:
  vmImage: ubuntu-latest

steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '3.11'

  - script: pip install fabric-cicd ruff pytest mlflow
    displayName: Install

  - script: ruff check .
    displayName: Lint notebooks & helpers

  - script: python -c "import yaml; yaml.safe_load(open('parameter.yml'))"
    displayName: Validate parameter.yml

  - script: pytest tests/ -q
    displayName: Unit tests
```

CI does **not** call Fabric — it only validates the repo. Anything that needs Spark/MLflow is exercised in the DEV workspace itself.

### 7.2 `pipelines/cd-ppe.yml` — auto-deploy `main` to PPE

```yaml
trigger:
  branches:
    include: [main]
  paths:
    include:
      - workspace/*
      - parameter.yml
      - deploy/*
      - pipelines/cd-ppe.yml

pr: none

variables:
  - group: fabric-common
  - group: fabric-ppe

pool:
  vmImage: ubuntu-latest

stages:
  - stage: Deploy_PPE
    jobs:
      - deployment: deploy_ppe
        environment: ppe
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self

                - task: UsePythonVersion@0
                  inputs:
                    versionSpec: '3.11'

                - script: pip install fabric-cicd
                  displayName: Install fabric-cicd

                - task: AzureCLI@2
                  displayName: Deploy to PPE workspace
                  inputs:
                    azureSubscription: $(AZURE_SUBSCRIPTION)   # workload-identity ARM service connection
                    scriptType: bash
                    scriptLocation: inlineScript
                    inlineScript: |
                      python deploy/deploy.py \
                        --environment PPE \
                        --workspace-id $(FABRIC_WORKSPACE_ID) \
                        --repo-dir $(REPO_DIR) \
                        --unpublish-orphans

                - task: AzureCLI@2
                  displayName: Smoke test - run PPE training pipeline
                  inputs:
                    azureSubscription: $(AZURE_SUBSCRIPTION)
                    scriptType: bash
                    scriptLocation: inlineScript
                    inlineScript: |
                      python deploy/run_pipeline.py \
                        --workspace-id $(FABRIC_WORKSPACE_ID) \
                        --pipeline-name train_and_register \
                        --wait
```

The `deployment` job tied to `environment: ppe` records every deploy on the Environment timeline and enables approvals/checks if you choose to add them.

### 7.3 `pipelines/cd-prod.yml` — manual gated promotion to PROD

```yaml
trigger: none      # manual only

parameters:
  - name: gitSha
    displayName: Commit SHA already validated in PPE
    type: string

variables:
  - group: fabric-common
  - group: fabric-prod

pool:
  vmImage: ubuntu-latest

stages:
  - stage: Deploy_PROD
    jobs:
      - deployment: deploy_prod
        environment: prod          # Environment has required-reviewer approval check
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                  fetchDepth: 0

                - script: git checkout ${{ parameters.gitSha }}
                  displayName: Check out validated SHA

                - task: UsePythonVersion@0
                  inputs:
                    versionSpec: '3.11'

                - script: pip install fabric-cicd
                  displayName: Install fabric-cicd

                - task: AzureCLI@2
                  displayName: Deploy to PROD workspace
                  inputs:
                    azureSubscription: $(AZURE_SUBSCRIPTION)
                    scriptType: bash
                    scriptLocation: inlineScript
                    inlineScript: |
                      python deploy/deploy.py \
                        --environment PROD \
                        --workspace-id $(FABRIC_WORKSPACE_ID) \
                        --repo-dir $(REPO_DIR)
```

The `environment: prod` reference causes Azure Pipelines to **pause** the run until the configured reviewers approve.

---

## 8. The ML-specific bit: model promotion via MLflow

`fabric-cicd` deploys **code**. Model artifacts are managed through MLflow's registry (built into Fabric), using a **centralized registry** pattern.

### Two URIs you can set independently

Fabric exposes MLflow at `azureml://workspaces/<ws-id>` per workspace, but MLflow itself has **two separate URIs** that you should configure independently:

| URI | What it controls |
|---|---|
| **Tracking URI** (`mlflow.set_tracking_uri`) | Where experiments, runs, params, metrics, and run artifacts are written |
| **Registry URI** (`mlflow.set_registry_uri`) | Where registered models and versions live |

**Recommended split:** point **tracking** at the workspace where the notebook is running, and point **registry** at the central `mlops-registry` workspace. This keeps team experiment data local while centralizing what really matters for promotion — the registered models.

### Centralized registry — the recommended pattern

Designate **one Fabric workspace** as the registry of record (e.g. `mlops-registry`). No notebooks need to run there — it exists purely to host the MLflow model registry. Every training notebook keeps its runs local but registers models into the shared registry:

```python
import mlflow

# Runs / metrics / params stay in the workspace where the notebook executes
# (Fabric auto-sets the tracking URI to the current workspace, but be explicit)
mlflow.set_tracking_uri(f"azureml://workspaces/{CURRENT_WS_ID}")

# Only registered models go to the central registry
mlflow.set_registry_uri(f"azureml://workspaces/{REGISTRY_WS_ID}")

mlflow.set_experiment("/experiments/churn")    # an experiment in the LOCAL workspace

with mlflow.start_run() as run:
    ...train + log metrics + log model...
    mlflow.sklearn.log_model(
        model, "model",
        registered_model_name="churn_model",    # registers in REGISTRY_WS_ID
    )
```

After PPE validation passes, tag the new version with the `challenger` alias on the **registry**:

```python
from mlflow.tracking import MlflowClient
client = MlflowClient(registry_uri=f"azureml://workspaces/{REGISTRY_WS_ID}")
client.set_registered_model_alias("churn_model", "challenger", new_version.version)
```

Promotion to PROD is then **just an alias flip — no artifact copy, no re-registration**:

```python
client.set_registered_model_alias(
    "churn_model", "champion",
    client.get_model_version_by_alias("churn_model", "challenger").version,
)
```

PROD scoring notebooks load by alias and never change (they only need the registry URI):

```python
import mlflow
mlflow.set_registry_uri(f"azureml://workspaces/{REGISTRY_WS_ID}")
model = mlflow.pyfunc.load_model("models:/churn_model@champion")
```

**Why split tracking and registry:**

| Concern | Split (tracking=local, registry=central) |
|---|---|
| Run history | Stays in DEV/PPE/PROD where the work happened — natural UX for data scientists |
| Permissions on raw experiments | Owned by the team that ran them |
| OneLake storage for runs | Billed to the team workspace |
| Model versions | Single source of truth in `mlops-registry` |
| Promotion latency | Instant — one API call alias flip |
| Atomicity | Single alias flip, succeeds or fails atomically |
| Rollback | Re-point alias to a previous version |
| Version numbering | Monotonic across the org (no drift between envs) |
| Storage cost for models | One copy of each artifact, ever |
| Audit | Single timeline of versions + alias changes in the registry |

**The one tradeoff:** a registered model version stores a back-reference to its source run (`source = runs:/<run-id>/model`). With the split pattern that run lives in a different workspace, so clicking through "view source run" in the registry requires read access on the originating workspace. The model artifact bytes themselves are copied into the registry workspace and remain valid even if the source workspace is later deleted.

**Simpler alternative:** small teams can point **both** URIs at `mlops-registry`. Everything ends up in one place — easier mental model, but experiment data from every team accumulates centrally and permissions get awkward as the org grows.

**Permissions model:**
- All training workspace identities: **Contributor** on `mlops-registry` (needed to register new model versions).
- All scoring workspace identities: **Viewer** on `mlops-registry`.
- The CI/CD promotion service principal: **Contributor** on `mlops-registry`.
- No one edits the registry workspace through the UI.

---

## 8a. Concrete code: promoting a trained model from PPE → PROD

`fabric-cicd` does **not** copy registered models between workspaces — it ships *code*. With the centralized-registry pattern, promotion is just an alias flip. Fabric exposes a per-workspace MLflow endpoint of the form `azureml://workspaces/<workspace-id>` that any `MlflowClient` can point at.

### Option B — Centralized registry alias flip (recommended)

Save as `deploy/promote_model.py`. Run it from the Azure DevOps PROD stage after approval.

```python
# deploy/promote_model.py
import os
from mlflow.tracking import MlflowClient

MODEL_NAME   = "churn_model"
REGISTRY_URI = f"azureml://workspaces/{os.environ['REGISTRY_WS_ID']}"
SOURCE_ALIAS = os.getenv("SOURCE_ALIAS", "challenger")
TARGET_ALIAS = os.getenv("TARGET_ALIAS", "champion")

# Promotion only touches the registry — no tracking URI needed
client = MlflowClient(registry_uri=REGISTRY_URI)

# 1. Resolve the validated source version
src = client.get_model_version_by_alias(MODEL_NAME, SOURCE_ALIAS)
print(f"Promoting {MODEL_NAME} v{src.version} (run {src.run_id})")

# 2. Sanity-check the metric tag that the PPE evaluator stamped on the version
#    (run metrics live in the source workspace's tracking server, so we use a
#     model-version tag set during the challenger step instead)
auc = float(src.tags.get("auc", 0))
threshold = float(os.getenv("MIN_AUC", "0.80"))
if auc < threshold:
    raise SystemExit(f"Refusing to promote: AUC {auc} < {threshold}")

# 3. Flip the alias atomically
client.set_registered_model_alias(MODEL_NAME, TARGET_ALIAS, src.version)
print(f"{MODEL_NAME}@{TARGET_ALIAS} -> v{src.version}")

# 4. Audit trail
client.set_model_version_tag(
    MODEL_NAME, src.version,
    key="promoted_by_pipeline",
    value=os.getenv("BUILD_BUILDNUMBER", "manual"),
)
```

PROD scoring is unchanged:

```python
import mlflow
mlflow.set_tracking_uri(f"azureml://workspaces/{REGISTRY_WS_ID}")
model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
predictions = model.predict(df)
```

### Option A — Per-workspace registry copy (fallback)

Use this only when you have a hard isolation requirement that prevents a shared registry — e.g. **different tenants**, **sovereign-cloud boundaries**, **regulatory air-gap requiring PROD to have zero non-PROD dependencies**, or **PROD identities that must not be able to read PPE metadata**.

```python
# deploy/promote_model_copy.py
import os
import mlflow
from mlflow.tracking import MlflowClient

MODEL_NAME   = "churn_model"
SOURCE_WS_ID = os.environ["SOURCE_WS_ID"]
TARGET_WS_ID = os.environ["TARGET_WS_ID"]
SOURCE_ALIAS = os.getenv("SOURCE_ALIAS", "challenger")
TARGET_ALIAS = os.getenv("TARGET_ALIAS", "champion")

def ws_uri(workspace_id: str) -> str:
    return f"azureml://workspaces/{workspace_id}"

src = MlflowClient(tracking_uri=ws_uri(SOURCE_WS_ID))
dst = MlflowClient(tracking_uri=ws_uri(TARGET_WS_ID))

src_version = src.get_model_version_by_alias(MODEL_NAME, SOURCE_ALIAS)

mlflow.set_tracking_uri(ws_uri(SOURCE_WS_ID))
local_path = mlflow.artifacts.download_artifacts(artifact_uri=src_version.source)

mlflow.set_tracking_uri(ws_uri(TARGET_WS_ID))
mlflow.set_experiment("/promotions")
with mlflow.start_run(run_name=f"promote-{MODEL_NAME}-v{src_version.version}") as run:
    mlflow.set_tags({
        "promoted_from_workspace": SOURCE_WS_ID,
        "promoted_from_version":   src_version.version,
        "promoted_from_run_id":    src_version.run_id,
    })
    mlflow.log_artifacts(local_path, artifact_path="model")
    new_version = mlflow.register_model(
        f"runs:/{run.info.run_id}/model",
        MODEL_NAME,
    )

dst.set_registered_model_alias(MODEL_NAME, TARGET_ALIAS, new_version.version)
```

Costs of this pattern: artifact bytes are copied for every promotion, version numbers diverge between workspaces, lineage becomes tag-based instead of native, and rollback requires re-promoting through the full pipeline.

### Wiring promotion into the Azure DevOps pipeline

Add a stage that runs after `Deploy_PPE` and depends on PROD-environment approval:

```yaml
- stage: Promote_To_PROD
  dependsOn: Deploy_PPE
  jobs:
    - deployment: promote_model
      environment: prod        # requires manual approval
      strategy:
        runOnce:
          deploy:
            steps:
              - task: UsePythonVersion@0
                inputs:
                  versionSpec: '3.11'

              - script: pip install mlflow azure-identity
                displayName: Install MLflow

              - task: AzureCLI@2
                displayName: Flip champion alias
                inputs:
                  azureSubscription: $(AZURE_SUBSCRIPTION)
                  scriptType: bash
                  scriptLocation: inlineScript
                  inlineScript: python deploy/promote_model.py
                env:
                  REGISTRY_WS_ID: $(REGISTRY_WORKSPACE_ID)
                  SOURCE_ALIAS:   challenger
                  TARGET_ALIAS:   champion
                  MIN_AUC:        "0.80"
```

The `AzureCLI@2` task with your workload-identity ARM service connection gives the script a token that MLflow picks up automatically via `DefaultAzureCredential`. The service principal needs **Contributor** on the registry workspace.

---

## 9. End-to-end flow

1. Data scientist commits a notebook change in the **git-connected DEV workspace** → branch pushed to Azure Repos → Pull Request opened.
2. `ci.yml` runs lint + unit tests via the **Branch policy → Build validation** rule on `main`.
3. PR completed → `cd-ppe.yml` triggers on `main`:
   - `fabric-cicd` publishes notebooks, pipelines, environment to **PPE**.
   - The PPE `train_and_register` pipeline runs, training a model and registering a new version in the centralized `mlops-registry`, tagged with alias `challenger`.
   - Evaluation notebook compares challenger vs current champion; results are surfaced as pipeline artifacts / test results.
4. A release manager queues `cd-prod.yml` with the validated commit SHA.
5. Reviewers approve in the `prod` Azure DevOps environment.
6. `fabric-cicd` deploys the same code to **PROD**.
7. `Promote_To_PROD` stage runs `deploy/promote_model.py`, flipping the `champion` alias on the centralized registry to point at the validated `challenger` version — atomic, instant, no artifact copy.
8. `nightly_scoring.DataPipeline` in PROD always loads `models:/churn_model@champion` from the centralized registry.

---

## 10. Operational tips

- **Pin the library version**: `pip install fabric-cicd==<x.y.z>` for reproducibility.
- **Keep DEV git-connected, deploy to others** — do not git-connect PPE/PROD; you'll get conflicts.
- **Treat `parameter.yml` as code** — protect with branch policies + required reviewers; bad values break prod silently.
- **Don't deploy `MLModel` items** — they aren't supported and aren't a useful unit of source control; rely on MLflow registry + aliases for model versioning instead.
- **Disable `unpublish_all_orphan_items` in PROD** if other teams co-own the workspace.
- **Use Pipeline caching** (`Cache@2` keyed on `requirements.txt`) to keep deploys under 60 seconds.
- **Monitor with Reflex** — wire a `Reflex` item to call the Azure DevOps REST API to queue a retraining pipeline when drift or latency breaches a threshold.
- **Secrets**: prefer **workload identity federation** on the ARM service connection; if you must use client secrets, store them in a **variable group backed by Azure Key Vault**, scoped per environment so PROD secrets aren't readable from PPE jobs.
- **Branch policies on `main`**: require PR, require the `ci.yml` build to pass, require at least one reviewer.

---

## 11. References

- fabric-cicd docs: <https://microsoft.github.io/fabric-cicd/>
- fabric-cicd repo: <https://github.com/microsoft/fabric-cicd>
- Parameterization reference: <https://microsoft.github.io/fabric-cicd/latest/how_to/parameterization/>
- Fabric Git integration (Azure DevOps): <https://learn.microsoft.com/fabric/cicd/git-integration/intro-to-git-integration>
- Machine learning model (Fabric): <https://learn.microsoft.com/fabric/data-science/machine-learning-model>
- Machine learning experiment (Fabric): <https://learn.microsoft.com/fabric/data-science/machine-learning-experiment>
- Manage MLflow models across Fabric workspaces: <https://learn.microsoft.com/fabric/data-science/machine-learning-cross-workspace-logging>
- MLflow autologging in Fabric: <https://learn.microsoft.com/fabric/data-science/mlflow-autologging>
- Azure DevOps workload identity federation: <https://learn.microsoft.com/azure/devops/pipelines/library/connect-to-azure>
- Azure Pipelines Environments & approvals: <https://learn.microsoft.com/azure/devops/pipelines/process/environments>
