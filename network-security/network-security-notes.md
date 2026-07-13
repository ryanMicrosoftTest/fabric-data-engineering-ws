# Microsoft Fabric Network Security — Deep Dive Notes

> Source: *Fabric Network Security – Everything You Need to Know* (External deck), Micah Rowland, Sr. Solutions Engineer, April 2026 (30 slides).

This document consolidates the deck into a structured reference covering inbound protection, tenant- and workspace-level Private Link, IP firewall, outbound protection, and supporting features (Managed Private Endpoints, Trusted Workspace Access, Workspace Identity).

---

## 1. Foundations

### 1.1 Fabric Security Layers (defense-in-depth)
From the data outward:

1. **Your data in Fabric** — encryption at rest and in transit (TLS 1.2+).
2. **Data security** — sensitivity / classification (Purview info-protection labels).
3. **Workspace & item security** — RBAC, item-level permissions.
4. **Inbound & outbound network security** — Private Link, Conditional Access, IP firewall, outbound access protection.
5. Surrounded by: **regulations & certifications, HA/DR, E2E auditability with Purview, advanced Purview tools, 3rd-party / in-house governance**.

### 1.2 Fabric Architecture Recap
- Fabric is a **SaaS** product.
- Users hit **Front-End Services**; **Entra ID** authenticates and trusts every request.
- Compute clusters live behind **back-end services** in Microsoft-managed VNets.
- Inter-experience traffic stays on the **Microsoft backbone**.
- All ingress is at least **TLS 1.2**.

### 1.3 Common Customer Requirements
- Outbound to data behind a customer firewall / Private Link Service.
- Inbound controls on user access to the platform and underlying resources.
- Secure access to backend services.
- Especially for **FSI / HLS** customers:
  - Traffic must be **private** (no public internet).
  - Endpoints must **not** be exposed to the public internet.

---

## 2. Inbound Network Protection

Two complementary options:

| Option | Layer | What it controls |
|---|---|---|
| **Azure Private Link** (Customer VNet) | Network | Fabric is reachable via a private IP from your VNet/Express Route/VPN. |
| **Entra Conditional Access (CA)** | Identity | Per-user/sign-in policies (IP, location, managed device, risk, MFA). |

### 2.1 Entra ID as the primary security boundary
- MFA + passwordless support.
- Conditional Access policies based on context/risk (**Entra ID P1 / E3** required).
- UEBA for identity-compromise detection (**Entra ID P2 / E5** required).
- Per the deck: *"Entra ID is the primary security boundary for controlling access to Fabric."*

### 2.2 Conditional Access — important gotchas
- **There is no single "Fabric" CA resource.** You must target each backend workload individually:
  - OneLake
  - Microsoft Fabric
  - Azure SQL Database
  - Azure Data Explorer (may appear as **Kusto** in older tenants)
  - Azure Storage
  - Power BI Service
  - Azure Cosmos DB
- If a target service has never been provisioned in the tenant, the CA resource won't be available — *create a minimal instance and delete it* to instantiate the service principal.
- **Continuous Access Evaluation (CAE) is not supported.** Tokens are honored until renewal — a user who logged in on-prem and went home retains access.
- A CA policy that **blocks one of the dependent target resources** will break Fabric items that depend on that service.
- Common decisions: **Block / Grant / Require MFA**.

---

## 3. Tenant-Level Private Link

### 3.1 What it does
- Disconnects Fabric from the public internet at the **tenant** scope.
- Every user must reach Fabric over the **private network on every device**.
- Resources can no longer be loaded locally → **slower reports**.
- Increases ExpressRoute bandwidth and adds Private Link cost.
- Brings several **product limitations** (see 3.3 / 3.4).

### 3.2 High-Level Setup
1. Configure **Advanced Networking** in Tenant Settings.
2. Create a **Private Link Service for Power BI** via custom template deployment.
3. Create the **VNet + Private Endpoint + Private DNS Zone**.
4. Validate connectivity over **private IPs**.
5. **Disable public access**.

Topology: On-prem → ExpressRoute/VPN → Azure VNets (peering) → Customer VNet1 → **Private Endpoint → OneLake / Microsoft Fabric**, with Conditional Access still applied at Entra layer.

### 3.3 Workload Limitations (workload-specific)

**OneLake**
- Direct API calls only supported on the **global endpoint** → data may leave its region.
- **No cross-tenant shortcut** support; external sources must support PE access or be reached from the private network.
- Cross-tenant access to OneLake via shortcut or data sharing is not supported.

**Warehouse / Pipelines**
- **Visual query** in Warehouse not supported.
- **Pipelines can't copy data into or out of a Warehouse**.
- Workspace migration across capacities in **different regions** isn't supported once a managed VNet is allocated to a workspace (auto-allocated on first Spark job/notebook run).

**Power BI features not supported**
- Semantic models with data sources of *Semantic Model* or *Dataflow*.
- **Publish to Web**.
- **Email Subscriptions**.
- **Export to PDF or PPTX**.
- **Usage Metrics** reporting incomplete.
- **Copilot**.

**Eventstreams / Activator**
- Eventstreams **don't support custom endpoints** as source/destination, nor Activator/Eventhouse as destination.
- Data Activator **doesn't support ingestion from Eventstream**.

**Eventhouse — not supported**
- Ingesting from OneLake.
- Shortcut to Eventhouse.
- Connecting to Eventhouse from Pipeline.
- Queued ingestion.
- T-SQL querying.
- **Azure event sources are blocked.**

**Other**
- **Purview Information Protection** not supported (can be configured via service tags — undocumented).
- **Mirroring** restricted to: Open Mirroring, Cosmos DB, Azure SQL MI, SQL Server 2025.
- **API for GraphQL**: API monitoring dashboard and Workspace-Monitoring-based logging not supported; SPN-created saved credentials and cross-region data source artifacts not supported.

### 3.4 Other (tenant-wide) Limitations
- **450 capacity limit**.
- New capacities take up to **24 hours** to become accessible (DNS replication).
- **Tenant migration not supported.**
- **Multi-tenant access** for users not possible (DNS).
- **No trial capacity** support.
- **External images / themes blocked.**
- **No cross-tenant scenarios** at all.
- **On-premises data gateways not supported** with PL enabled.
- **VNet data gateway "download diagnostics" not supported.**
- **Microsoft Fabric Capacity Metrics** app not available.
- **OneLake Catalog → Govern tab** not available.

---

## 4. Workspace-Level Private Link

### 4.1 Overview
- Apply Private Link to **selected workspaces** rather than the whole tenant.
- Selected workspaces are closed off from the public internet.
- **Tenant-wide PL limitations don't apply**, but workspace-PL has its own.
- **Deployment Pipelines** and **Default Semantic Models** are **not supported** — any unsupported workload present **blocks** enabling PL on the workspace.
- Connecting via API requires a **complicated URL**.

Topology: customer's VNet/Private Endpoint → **Azure Private Link (Workspace Level)** → Workspace A (Lakehouse / Warehouse / Notebook / SJD / OneLake) and Workspace B (Semantic Model / Pipeline / Report / KQL DB), with tenant-level Entra CA still in effect.

### 4.2 Workspace-Level PL Feature Support Matrix (April 2026)

Legend: ✅ supported · ⚠️ supported with limitations · ❌ unsupported / disabled

#### Get Data
| Item | Status |
|---|---|
| Copy Job | ✅ |
| Dataflow Gen1 | ❌ |
| Dataflow Gen2 | ⚠️ |
| Eventstream | ⚠️ |
| Notebook | ✅ |
| Pipeline | ⚠️ |
| Spark Job Definition | ✅ |

#### Mirror Data
| Item | Status |
|---|---|
| Azure Cosmos DB | ✅ |
| Azure Database for MySQL | ❌ |
| Azure Database for PostgreSQL | ❌ |
| Azure Databricks catalog | ❌ |
| Azure SQL Database | ❌ |
| Azure SQL Managed Instance | ✅ |
| Database (Fabric) | ❌ |
| Google BigQuery (preview) | ❌ |
| Oracle | ❌ |
| SAP | ❌ |
| SharePoint Online List | ❌ |
| Snowflake | ❌ |
| SQL Server 2025 | ✅ |

#### Store Data
| Item | Status |
|---|---|
| Cosmos DB database | ❌ |
| Datamart (preview) | ❌ |
| Eventhouse | ⚠️ |
| Lakehouse | ✅ |
| Sample warehouse | ✅ |
| Semantic model | ❌ |
| Snowflake database | ❌ |
| SQL database | ❌ |
| Warehouse | ✅ |

#### Prepare Data
| Item | Status |
|---|---|
| Apache Airflow job | ❌ |
| Azure Data Factory | ✅ |
| Dataflow Gen1 | ❌ |
| Dataflow Gen2 | ✅ |
| dbt job (preview) | ❌ |
| Eventstream | ⚠️ |
| Notebook | ✅ |
| Pipeline | ⚠️ |
| Spark Job Definition | ✅ |

#### Visualize Data
| Item | Status |
|---|---|
| Dashboard | ❌ |
| Exploration (preview) | ❌ |
| Graph model (preview) | ❌ |
| Graph queryset (preview) | ❌ |
| Map | ❌ |
| Paginated Report (preview) | ❌ |
| Real-Time Dashboard | ❌ |
| Report | ❌ |
| Scorecard | ❌ |

> All visualization items currently disabled under workspace-level PL.

#### Analyze and Train Data
| Item | Status |
|---|---|
| Anomaly detector (preview) | ❌ |
| Data agent | ❌ |
| Environment | ✅ |
| Experiment | ✅ |
| Graph model (preview) | ❌ |
| ML model | ✅ |
| Notebook | ✅ |
| Ontology (preview) | ❌ |
| Operations agent (preview) | ❌ |
| Spark Job Definition | ✅ |

#### Develop Data
| Item | Status |
|---|---|
| API for GraphQL | ❌ |
| Environment | ✅ |
| Notebook | ✅ |
| Plan (preview) | ❌ |
| User data function | ❌ |
| Variable Library | ⚠️ |

#### Track Data
| Item | Status |
|---|---|
| Activator | ❌ |
| Anomaly detector (preview) | ❌ |
| Digital Twin Builder (preview) | ❌ |
| Eventhouse | ⚠️ |
| Eventstream | ⚠️ |
| KQL Queryset | ❌ |
| Map | ❌ |
| Operations agent (preview) | ❌ |
| Scorecard | ❌ |

#### Distribute Data
| Item | Status |
|---|---|
| Org app (preview) | ❌ |

#### Other
| Item | Status |
|---|---|
| Healthcare data solutions | ❌ |
| Streaming dataflow | ❌ |
| Streaming dataset | ❌ |
| Sustainability solution | ❌ |

#### Platform Features
| Item | Status |
|---|---|
| Deployment pipelines | ❌ |
| Default Semantic Models | ❌ |

### 4.3 Other Workspace-PL Limitations
- **Not supported**: Item sharing, Shortcut transforms, Pipeline workspace staging and copy to Eventhouse.
- **Eventstream** source/destination support is limited.
- **Eventhouses** cannot consume events from Eventstreams or use SQL Server **TDS endpoints**; additional limits around ML, Eventstream polling, Azure Event Hubs integration, and Queued ingestion via OneLake.
- **Dataflow Gen2** requires a **VNet data gateway**; no cross-dataflow connections.
- **Variable Library** items not accessible by Pipelines.
- **Mirroring**: Open Mirroring, CosmosDB, Azure SQL MI, SQL Server 2025 only.
- **Cross-workspace event consumption** of Azure & Fabric events requires cross-workspace setup.
- **OneLake Catalog → Govern tab** unavailable.
- **OneLake Security** and **Workspace Monitoring** unsupported.
- **Warehouse connections require URL changes.**
- A workspace **cannot be deleted** until its Private Link Service is deleted.
- **Limits**:
  - **100** private endpoints / workspace.
  - **500** PL-enabled workspaces / tenant.
  - **10** Private Link Services / minute creation rate.
- Enabling **Inbound and Outbound** protections requires the **API** to configure.

### 4.4 Tenant ↔ Workspace PL Interactions
The deck includes diagrams (slides 19–21) covering:
- Coexistence of tenant- and workspace-level Private Link in the same tenant.
- **Cross-tenant** connections using workspace-level Private Links.
- **Cross-workspace** connection patterns.

---

## 5. Workspace-Level IP Firewall

Extends workspace-level Private Link to **public IPs**.

**Limitations**
- **Public IPs only** — cannot add IPs of Azure VMs that sit on VNets with Private Endpoints.
- **256 rules / workspace.**
- **Does not support**:
  - Databricks Unity Catalog
  - OneLake Security
  - Power BI
  - Copilot experiences

---

## 6. Outbound Network Protection

### 6.1 Workspace Outbound Access Protection (overview)
- Workspace-scoped control to **restrict outbound access** to the public internet.
- Restricts outbound connections to **permitted destinations only**.
- Designed to be combined with other security features for robust outbound control.
- **Roadmap (per deck)**: Q2 2025 for **Spark**, other workloads in future milestones.

### 6.2 Getting Data Into Fabric (private patterns)
The deck maps inbound paths into a private workspace:

- **Azure PaaS sources** (ADF, Azure Databricks, Synapse Spark) →
  - Fabric Pipelines (COPY)
  - Dataflows Gen2
- **On-prem** →
  - **On-Prem Data Gateway**
  - **VNet Data Gateway**
- **Fabric Spark** with **Managed Private Endpoints** (all F SKUs).
- **Fabric Eventstreams**.
- **Shortcut to ADLS Gen2** + Fabric Pipelines (COPY) + `COPY INTO` for Warehouse.
- **Trusted Workspace Access** (all F SKUs).
- **OneLake Shortcuts** to **Amazon S3, GCS, OneLake, on-prem S3-compatible** stores.

### 6.3 Outbound Protection — Data Engineering
- **Supported items**: Lakehouses, Notebooks, Spark Job Definitions, Environments.
- **Important considerations / limitations**:
  - Installing Python packages requires either **wheel files uploaded as artifacts to an Environment** or a **privately hosted PyPI mirror on Azure Storage**.
  - **Schema-enabled Lakehouses** in a protected workspace **cannot be accessed via Spark SQL statements**.

### 6.4 Outbound Protection — Data Factory
- **Supported items**: Dataflow Gen2 (with CI/CD), Pipelines, Copy Jobs.
- **Pipelines connector support**: `FabricDataPipeline`, `CopyJob`, `userDataFunction`, `PowerBIDataset`.
- **Not supported**: Teams or O365 Outlook activity.
- **Cross-workspace dataflows** don't support **Data Warehouse destinations**.
- See docs for additional considerations.

### 6.5 Outbound Access Monitoring
Fabric tracks user activity for **connection creation and use** in usage logs and **Purview Audit**, e.g.:
- Create cloud connections
- Create notebooks / pipelines
- Create shortcuts

References:
- *Track user activities in Microsoft Fabric* (Microsoft Learn)
- *Data connection auditing for exfiltration protection* (Microsoft Learn)

---

## 7. Trusted Workspace Access (TWA)

### 7.1 Where TWA can be used
- **OneLake shortcuts**
- **Pipelines**
- **Semantic models**
- **T-SQL `COPY` statement**
- **AzCopy**

### 7.2 Important Considerations
- **Pipelines can't write to OneLake table shortcuts.**
- **Reusing connections** that support TWA may fail.
- **Not compatible with cross-tenant requests.**
- See docs for full details and ARM templates.

### 7.3 Pattern: Firewall-protected ADLS Gen2 via Workspace Identity
- A **Resource Instance Rule** on the firewall-protected ADLS Gen2 account permits *Workspace A's* Workspace Identity to access the storage account, enabling private-style access without opening the firewall to public IPs.

---

## 8. Quick Decision Guide

| Requirement | Recommended control |
|---|---|
| Restrict who can sign in (device, location, MFA) | **Entra Conditional Access** (target each backend resource) |
| Keep all Fabric traffic off the public internet, tenant-wide | **Tenant-level Private Link** (accept broad limitations) |
| Lock down only specific workspaces | **Workspace-level Private Link** (check the support matrix in §4.2) |
| Allow specific public IPs into a private workspace | **Workspace-level IP Firewall** (no PBI / OneLake Security / Copilot / Databricks UC) |
| Restrict where Fabric can call out | **Workspace Outbound Access Protection** (Spark first; Q2 2025) |
| Reach a firewall-protected Azure data source from Fabric | **Managed Private Endpoints** + **Workspace Identity** + **Trusted Workspace Access** |
| Audit connection / exfiltration risk | **Purview Audit** + Fabric usage logs |

---

## 9. Reference Links (from the deck)

### Inbound & Identity
- [Protect inbound traffic (Private Link vs Conditional Access)](https://learn.microsoft.com/en-us/fabric/security/protect-inbound-traffic)
- [Conditional Access in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/security/security-conditional-access)

### Private Link (Inbound)
- [Private Links – Overview](https://learn.microsoft.com/en-us/fabric/security/security-private-links-overview)
- [Tenant-level Private Link](https://learn.microsoft.com/en-us/fabric/security/security-private-links-use)
- [Workspace-level Private Link – Overview](https://learn.microsoft.com/en-us/fabric/security/security-workspace-level-private-links-overview)
- [Workspace-level Private Link – Setup](https://learn.microsoft.com/en-us/fabric/security/security-workspace-level-private-links-set-up)
- [Tenant vs Workspace Private Link interaction](https://learn.microsoft.com/en-us/fabric/security/security-tenant-workspace-private-links-same-tenant)

### Outbound & Private Access
- [Workspace outbound access protection – Overview](https://learn.microsoft.com/en-us/fabric/security/workspace-outbound-access-protection-overview)
- [Enable outbound access protection](https://learn.microsoft.com/en-us/fabric/security/workspace-outbound-access-protection-set-up)
- [Managed private endpoints – Overview](https://learn.microsoft.com/en-us/fabric/security/security-managed-private-endpoints-overview)
- [Create and use managed private endpoints](https://learn.microsoft.com/en-us/fabric/security/security-managed-private-endpoints-create)
- [Trusted workspace access](https://learn.microsoft.com/en-us/fabric/security/security-trusted-workspace-access)
- [Workspace identity](https://learn.microsoft.com/en-us/fabric/security/workspace-identity)
- [Authenticate with workspace identity](https://learn.microsoft.com/en-us/fabric/security/workspace-identity-authenticate)

> Note (from the deck): Feature availability varies by **SKU, region, and configuration**. Always validate against current Microsoft Learn docs before designing.
