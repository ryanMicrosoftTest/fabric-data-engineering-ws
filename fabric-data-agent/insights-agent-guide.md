# Building an Insights Data Agent in Microsoft Fabric

A practitioner's end-to-end guide for designing, building, evaluating, publishing, consuming, and monitoring a **Fabric Data Agent** over a **Lakehouse** data source.

The guide uses a clinical-notes lakehouse (`health_dbo.clinical_notes`) as a running worked example, but every step applies to any insights agent over any lakehouse.

> **Audience:** Fabric data engineers / data scientists building the agent hands-on.
> **Data source scope:** Lakehouse (SQL endpoint).
> **Status:** Fabric Data Agent and the `fabric-data-agent-sdk` are in **preview** as of writing; APIs may change.

---

## Table of contents

1. [What is an Insights Data Agent?](#1-what-is-an-insights-data-agent)
2. [Capabilities — what it can and can't do](#2-capabilities--what-it-can-and-cant-do)
3. [Prerequisites](#3-prerequisites)
4. [Phase 1 — Design](#4-phase-1--design)
5. [Phase 2 — Prepare the lakehouse](#5-phase-2--prepare-the-lakehouse)
6. [Phase 3 — Create the data agent](#6-phase-3--create-the-data-agent)
7. [Phase 4 — Configure data source, instructions, and examples](#7-phase-4--configure-data-source-instructions-and-examples)
8. [Phase 5 — Interactive testing in the Fabric UI](#8-phase-5--interactive-testing-in-the-fabric-ui)
9. [Phase 6 — Programmatic evaluation with the SDK](#9-phase-6--programmatic-evaluation-with-the-sdk)
10. [Phase 7 — Publish to production](#10-phase-7--publish-to-production)
11. [Phase 8 — Consumption patterns](#11-phase-8--consumption-patterns)
12. [Phase 9 — Monitoring and regression testing](#12-phase-9--monitoring-and-regression-testing)
13. [Phase 10 — Governance, security, and cost](#13-phase-10--governance-security-and-cost)
14. [Troubleshooting cheat sheet](#14-troubleshooting-cheat-sheet)
15. [References](#15-references)

---

## 1. What is an Insights Data Agent?

A **Fabric Data Agent** is an AI agent that:

- Accepts natural-language questions from a user (or another system).
- Translates them into SQL/DAX against one or more configured **data sources**.
- Executes the query against the source, gets results, and returns a natural-language (and/or tabular) answer.
- Uses **administrator-supplied instructions, example queries, and table/column descriptions** to ground its behavior.

An **insights agent** specifically targets analytical Q&A — counts, breakdowns, trends, top-N, comparisons, anomaly callouts — rather than transactional lookups.

In our worked example, the agent answers questions like:

- *"How many patients have at least one clinical note?"*
- *"What note types are most common?"*
- *"For each patient, what is the date of their most recent clinical note?"*

---

## 2. Capabilities — what it can and can't do

A common source of project pain is treating the Data Agent as a general-purpose AI assistant. It's specifically a **natural-language → SQL → answer** engine. Here's a concrete capability matrix so you can scope expectations before the kickoff meeting.

### ✅ What a Fabric Data Agent CAN do

| Capability | Notes |
|---|---|
| Translate natural language to SQL (T-SQL on Lakehouse/Warehouse, DAX on semantic models, KQL on Eventhouse) | The core feature. |
| Execute the generated query against the configured source(s) | Runs as the **caller's identity**, so RLS/CLS is honored. |
| Return the result as a table + a natural-language summary | Tabular output, plus prose. |
| Answer follow-up questions within the same thread | Conversational context is maintained per thread (not across threads). |
| Use multiple configured data sources in one agent | Up to a small number (~5). The agent picks the right one per question. |
| Choose between tables/views based on the question | Quality scales with table/column descriptions and example queries. |
| Apply business rules encoded in instructions and examples | Naming conventions, default ordering, tiebreakers, format rules. |
| Refuse out-of-scope or unsafe requests | Only as well as your guardrails — see §13. |
| Be called programmatically | Via `fabric-data-agent-sdk` (`FabricOpenAI` client) or REST. |
| Be surfaced inside Copilot in Power BI | As a skill. The visual is rendered by Copilot, not the agent. |
| Be composed by other agents | Foundry / orchestrator agents can call a published Data Agent as a tool. |
| Honor delegated auth and per-row security | The agent never bypasses RLS/CLS configured on the source. |
| Show its reasoning | Generated SQL/DAX, tool calls, and intermediate results are visible in the thread / `thread_url`. |
| Be evaluated programmatically | Via the SDK's `evaluate_data_agent` + ground-truth dataset (§9). |
| Be versioned (sandbox vs. production) | Promotion is explicit; consumers pin to a stage. |

### ❌ What a Fabric Data Agent CANNOT do (today)

| Capability | What's actually true | If you need this… |
|---|---|---|
| **Create visualizations / charts** | Returns tables and text, never a chart object. | Consume the agent through **Copilot in Power BI** (Copilot renders the visual), or call the agent from a notebook/app and chart the data yourself (matplotlib/plotly). |
| **Write back to the data source** (INSERT / UPDATE / DELETE / MERGE) | Read-only by design. Even if you don't enforce it in instructions, the SQL endpoint will reject DML on Lakehouse SQL endpoint. | Build a separate pipeline / notebook / Foundry agent with write-capable tools. |
| **Modify schema** (CREATE / ALTER / DROP) | Same — read-only. | Use Fabric Data Engineering / DBT / pipelines. |
| **Schedule itself** to run on a cadence | The agent is request/response only. It doesn't "wake up." | Schedule a Fabric notebook or pipeline that *calls* the agent. |
| **Send emails, Teams messages, or other notifications** | No outbound actions. | Have your scheduled notebook post results via webhook / Teams / Outlook. |
| **Call external HTTP APIs** | No tool/function-calling to arbitrary endpoints. | Wrap the agent in a Foundry agent or app that orchestrates the HTTP calls. |
| **Generate downloadable Excel / PDF / CSV files** | Returns text + tables in the response payload. | Capture the response in a notebook and write the file yourself, or use Power BI export. |
| **Stream long-running output token-by-token** | Responses arrive when the full query + judgment completes. | Not avoidable today; design around it (loading state in UI). |
| **Query data sources that aren't explicitly configured** | If a table/view isn't in a configured data source, the agent can't see it. | Add the source or expose the data via a curated view. |
| **Join across data sources** | A single query targets a single source. The agent can run separate queries against different configured sources, but cannot do a cross-source SQL join. | Materialize the join into a lakehouse view first, then expose that view. |
| **Train, fine-tune, or call ML models** | Not an ML platform feature. | Use Fabric Data Science (notebooks, MLflow). |
| **Maintain memory across separate threads** | Each thread is isolated. The agent doesn't "remember" your previous conversation tomorrow. | Persist state in your calling app and feed it into the new thread. |
| **Bypass RLS / CLS** | Queries run as the caller. Restricted users see restricted results. | This is a feature, not a limitation — don't try to bypass. |
| **Perform predictive forecasting or "what-if" analysis** | It can only return what SQL can compute over the existing data. No models, no forecasts. | Add a forecasting model upstream and expose its outputs as a table. |
| **Search unstructured documents** (PDFs, Word, images) outside the lakehouse | The agent only sees configured Fabric data sources. | Use Azure AI Search or a separate Foundry RAG agent. |
| **Take actions in other systems** (file a Jira ticket, create a SharePoint item, etc.) | No action tools. | Compose with a Foundry agent that has those tools. |
| **Guarantee deterministic answers** | LLM-based generation means the same question may produce slightly different SQL / phrasing on different runs. | Lock down behavior via instructions, examples, and tight evaluation — but expect minor variance. |
| **Self-correct silently for ambiguous questions** | If the question is ambiguous (e.g., "most recent" with ties), the agent may pick differently each run. | Tighten the question or encode tiebreakers in instructions/examples. |
| **Replace a data warehouse, semantic model, or Power BI dashboard** | It's an *interface* on top of those things, not a substitute for them. | Continue investing in your curated data layer and reports. |

### Gray-area capabilities (works, with caveats)

| Capability | Caveat |
|---|---|
| Free-text search inside a column | Works via `LIKE` / `LOWER`, but slow on large unstructured text. Consider full-text indexing or an Azure AI Search front-end for serious text retrieval. |
| Returning long free-text fields (e.g., full memos) | Works, but the LLM-judge in your eval may struggle to grade full-paragraph matches reliably. Use single-fact probes for evaluation. |
| Multi-step reasoning ("find X, then for each X compute Y") | The agent can chain SQL calls within a thread, but complex multi-step plans are flakier than single-query questions. Reduce to one query where possible (CTEs, window functions). |
| Cross-language questions (non-English) | Generally works for the major Fabric-supported languages; quality varies. Test in your target language as part of evaluation. |
| Time-zone-aware answers | T-SQL on the Lakehouse SQL endpoint has limited tz functions. Normalize timestamps in your curated view rather than relying on the agent. |
| Sub-second latency | Typical response time is several seconds (LLM round-trip + SQL execution). Not appropriate for high-QPS or real-time UI hot paths. |

### A useful mental model

Think of a Fabric Data Agent as **"a very capable analyst who can only write SELECT statements against tables you've shown them, can only answer in text and tables, and forgets you between conversations."** Anything outside that — visuals, actions, schedules, write-back, cross-system orchestration — has to come from the surface you embed the agent into, or from a coordinating agent above it.

---

## 3. Prerequisites

| Requirement | Notes |
|---|---|
| **Fabric capacity** | Paid F2+ SKU, or P1+ Power BI Premium capacity with Fabric enabled. |
| **Workspace** | Capacity-assigned workspace where you have **Member** or higher. |
| **Tenant settings** | Admin must enable *Copilot in Microsoft Fabric* and *Data Agent* features, including cross-geo processing/storage for AI if your capacity region differs. |
| **Source data** | At least one Lakehouse with curated, query-ready Delta tables. |
| **Identity** | Azure AD account with read access to the source lakehouse. |
| **(Optional) Local dev** | Python 3.10–3.12 venv with `fabric-data-agent-sdk` for local eval / CI. (3.13 is not supported as of writing.) |

---

## 4. Phase 1 — Design

Resist the temptation to "just create the agent." A 30-minute design exercise prevents weeks of rework.

### 4.1 Define the agent's *job*

Write a one-sentence charter, e.g.:

> *"Answer ad-hoc analytical questions about clinical notes (who, when, what types, content lookups) for the clinical operations team."*

### 4.2 Inventory the data surface

For each table the agent will see, document:

- **Purpose** — one sentence.
- **Grain** — what does one row represent?
- **Key columns** — PKs, FKs, dates, categorical dimensions, free-text columns.
- **Volume & cardinality** — helps you predict query shapes the agent should support.
- **Sensitivity** — PHI, PII, financial, etc. (drives RLS/CLS decisions in §13).

Worked example:

| Table | Grain | Key columns | Sensitive? |
|---|---|---|---|
| `health_dbo.clinical_notes` | One clinical note | `note_id` (PK), `patient_id` (FK), `note_date`, `note_type`, `memo_content` | Yes — synthetic stand-in for PHI |

### 4.3 List the top 10–20 expected questions

This is the most valuable artifact in the entire project. It becomes:

- The basis for **example queries** you give the agent (§7.3).
- The basis for the **evaluation ground truth** (§9).
- The acceptance criteria for "is the agent good enough to ship."

Group them by capability:

| Capability | Sample question |
|---|---|
| Simple count | *How many patients have clinical notes?* |
| Scoped count | *How many notes does patient_4 have?* |
| Distinct list | *List all patient IDs with clinical notes.* |
| Grouped aggregation + ordering | *What note types exist and how common is each?* |
| Per-group latest (window function) | *For each patient, what is the date of their most recent note?* |
| Content lookup (free-text scan) | *Give me the most recent note for patient_5 mentioning hypertension.* |
| Single-fact extraction | *What is patient_5's MRN?* |

### 4.4 Decide what's **out of scope**

Explicitly write down what the agent will *not* answer. Examples:

- "Will not answer questions requiring data not in the lakehouse."
- "Will not generate clinical advice or interpretation."
- "Will not return raw PHI to unauthenticated callers."

These become guardrails in the agent's instructions (§7.2).

---

## 5. Phase 2 — Prepare the lakehouse

The agent is only as good as the lakehouse it queries. Do this work *before* you create the agent.

### 5.1 Use clean, semantic table and column names

The model uses names as primary clues. Prefer:

- `clinical_notes`, `patient_id`, `note_date` ✅
- `tbl_cn_01`, `pid`, `dt` ❌

If you can't rename source tables, create **views** in a curated schema (e.g., `health_dbo`) with friendly names.

### 5.2 Add table & column descriptions in the SQL endpoint

In the Fabric portal, open the lakehouse's **SQL analytics endpoint**, navigate to a table → **Properties** → add:

- A clear **table description** ("One row per clinical note authored for a patient.").
- A **column description** for every column (especially codes/enums — list the allowed values).

These descriptions are surfaced to the agent and dramatically improve grounding.

### 5.3 Prefer narrow, purpose-built tables/views

Don't point the agent at a 200-column kitchen-sink table. Build a view exposing only the columns it needs:

```sql
CREATE VIEW health_dbo.v_clinical_notes_agent AS
SELECT
    note_id,
    patient_id,
    note_date,
    note_type,
    memo_content
FROM health_dbo.clinical_notes;
```

Fewer columns → faster generation, fewer hallucinations, less PHI exposure.

### 5.4 Pre-compute hard joins / business logic

If a common question requires a 4-table join the agent struggles with, materialize it as a view. Treat the view as the **agent-friendly API** to your data.

### 5.5 Validate data quality

The agent will faithfully report bad data. Before exposing:

- Null/unknown handling — e.g., wrap with `ISNULL(note_type, '(unspecified)')` in views.
- Deduplication — duplicates cause ambiguous "most recent" answers (see §7.3 and §9).
- Consistent date types — `DATE`, not strings.

---

## 6. Phase 3 — Create the data agent

1. In the target workspace, click **+ New item** → search for **Data Agent** → create.
2. Name it with a clear convention, e.g., `clinical_insights_agent`.
   - Suggested naming: `<domain>_<purpose>_agent` (e.g., `finance_kpi_agent`, `ops_incident_agent`).
3. The agent opens in the **authoring canvas** with three panes:
   - **Configuration** (left) — data sources, instructions, example queries.
   - **Chat** (center) — interactive testing.
   - **Reasoning** (right) — generated SQL, tool calls, errors.

The agent is now in **sandbox** stage. Nothing is published until you explicitly do so (§10).

---

## 7. Phase 4 — Configure data source, instructions, and examples

This is where 80% of agent quality comes from.

### 7.1 Add the lakehouse data source

1. In the configuration pane, click **+ Data source** → **Lakehouse**.
2. Select your lakehouse (e.g., the one containing `health_dbo.clinical_notes`).
3. **Select only the tables/views the agent should see.** Uncheck everything else — agents pick the wrong table when there are too many choices.
4. (Optional) Override the auto-detected table/column descriptions if needed.

Worked example: select only `health_dbo.v_clinical_notes_agent`.

### 7.2 Write agent-level instructions

> 📋 **Looking for a copy-paste template?** Skip ahead to **[Appendix A — Reusable instructions template](#appendix-a--reusable-instructions-template)** for a full ROLE / DATA SOURCE / QUERY RULES / PRIVACY & SAFETY / RESPONSE STYLE / FEW-SHOT EXAMPLES skeleton you can drop into any insights agent and fill in.

Keep instructions short, declarative, and *prescriptive*. Cover:

- **Persona / scope**: what the agent does and for whom.
- **Source of truth**: which table(s) to use for what.
- **Conventions**: date formats, ordering rules, default filters.
- **Guardrails**: what to refuse or qualify.
- **Tone**: concise, no clinical advice, cite IDs when relevant.

Worked example:

```
You are an analytical assistant for the clinical operations team. Answer
questions about clinical notes using only the table
health_dbo.v_clinical_notes_agent.

Rules:
- One row in v_clinical_notes_agent represents one clinical note.
- "Patients with notes" means DISTINCT patient_id in v_clinical_notes_agent.
- When asked for "most recent", order by note_date DESC; break ties by
  note_id ASC.
- Format dates as YYYY-MM-DD.
- For per-group results, return data as a compact list, one row per group.
- Do not provide clinical interpretation, diagnoses, or treatment advice.
  If asked, respond that you only summarize stored notes.
- If the question requires data not in v_clinical_notes_agent, say so
  explicitly rather than guessing.
```

### 7.3 Add example queries (NL → SQL pairs)

Examples are the highest-leverage tuning you can do. For each capability you identified in §4.3, provide 1–2 examples:

```
Q: How many distinct patients have clinical notes?
A: SELECT COUNT(DISTINCT patient_id) AS patient_count
   FROM health_dbo.v_clinical_notes_agent;

Q: For each patient, what is the date of their most recent clinical note?
A: WITH ranked AS (
       SELECT patient_id, note_date,
              ROW_NUMBER() OVER (PARTITION BY patient_id
                                 ORDER BY note_date DESC, note_id ASC) AS rn
       FROM health_dbo.v_clinical_notes_agent
   )
   SELECT patient_id, note_date
   FROM ranked WHERE rn = 1
   ORDER BY patient_id;

Q: How common is each note type?
A: SELECT ISNULL(note_type, '(unspecified)') AS note_type,
          COUNT(*) AS note_count
   FROM health_dbo.v_clinical_notes_agent
   GROUP BY note_type
   ORDER BY note_count DESC;
```

Tips:

- Include **at least one example for every SQL pattern you expect** (joins, window functions, date math, text search).
- Encode your conventions (e.g., the tiebreaker rule) in the examples — the agent generalizes from them.
- Don't overload with 50 examples; ~10 high-quality ones beat 50 noisy ones.

### 7.4 Save and let the agent index

After saving, the agent re-indexes the configured sources. Wait for the status indicator to show ready before testing.

---

## 8. Phase 5 — Interactive testing in the Fabric UI

Before writing any code, sanity-check the agent in the chat pane.

1. Ask 5–10 questions from your §4.3 list.
2. For each, click into the **Reasoning** pane and inspect:
   - The generated SQL.
   - Which table(s) it picked.
   - Whether it followed your instructions (date format, tiebreaker, etc.).
3. When something is wrong, the fix is usually one of:
   - **Wrong table / column** → tighten descriptions or remove unused tables.
   - **Wrong logic** → add an example query covering that case.
   - **Wrong format / tone** → tighten instructions.

Iterate quickly here. Cheaper than fixing it after publishing.

---

## 9. Phase 6 — Programmatic evaluation with the SDK

Interactive testing scales poorly and doesn't catch regressions. Set up a repeatable evaluation harness with the `fabric-data-agent-sdk`.

### 9.1 Create a dedicated evaluation lakehouse

Don't pollute your source lakehouse with eval results. In the workspace, create a lakehouse like **`lh_data_agent_eval`** and attach it as the **default lakehouse** of your evaluation notebook only.

This lakehouse will hold one pair of Delta tables per agent:

- `<agent>_evaluation_output` — one row per run (summary, accuracy).
- `<agent>_evaluation_output_steps` — one row per question per run (reasoning, generated SQL, judgment, thread URL).

### 9.2 Install the SDK

In a notebook attached to your Fabric workspace:

```python
%pip install -U fabric-data-agent-sdk
```

For local dev (Python 3.10–3.12 only):

```powershell
uv venv --python 3.12 .venv
.\.venv\Scripts\Activate.ps1
uv pip install -U fabric-data-agent-sdk
```

### 9.3 Build the ground-truth dataset

Rules learned the hard way:

- `question` is the **natural-language question** (not SQL).
- `expected_answer` is the **literal value(s) the agent should return** (not SQL, not the table).
- Keep answers **short and deterministic** — scalars, small lists, or `key: value` pairs.
- Pin down ambiguity: if "most recent" has ties, specify the tiebreaker in the question.
- Don't paste UI escape glyphs (`↵`) into strings — use real `\n`.

Worked example:

```python
import pandas as pd

df = pd.DataFrame(
    columns=['question', 'expected_answer'],
    data=[
        ["List all patient IDs that have clinical notes.",
         "patient_4, patient_5, patient_6, patient_7"],

        ["How many patients have clinical notes?", "4"],

        ["How many clinical notes does patient_4 have?", "16"],

        ["What note types exist in clinical_notes and how common is each? "
         "Order from most to least common.",
         "Progress Notes: 12, Multidisciplinary Tumor Board Discussion: 6, "
         "Telephone Encounter: 5, Pathology and Cytology: 4, Lab Results: 3, "
         "Initial Consult: 3, Patient Instructions: 3, Radiology Report: 3, "
         "Surgical Operative Note: 2, Treatment Plan: 2, Procedures: 1, "
         "CT: 1, PET: 1"],

        ["For each patient, what is the date of their most recent clinical note? "
         "List as 'patient_id: date' pairs.",
         "patient_4: 2021-03-27, patient_5: 2025-03-12, "
         "patient_6: 2025-02-01, patient_7: 2024-11-05"],

        ["For each patient, what is the note_type of their most recent clinical note? "
         "Break ties by note_id ascending. List as 'patient_id: note_type' pairs.",
         "patient_4: Progress Notes, "
         "patient_5: Multidisciplinary Tumor Board Discussion, "
         "patient_6: Multidisciplinary Tumor Board Discussion, "
         "patient_7: Multidisciplinary Tumor Board Discussion"],

        # Single-fact memo probes — robust alternatives to full-memo matching
        ["What is patient_5's MRN according to their clinical notes?", "123456"],
        ["What medication and dose is patient_5 taking for hypertension?",
         "lisinopril 10 mg daily"],
    ]
)
```

Store this DataFrame in a CSV in the repo so it's version-controlled:

```python
df.to_csv("/lakehouse/default/Files/eval/clinical_insights_agent_truth.csv", index=False)
```

### 9.4 Run the evaluation

```python
from fabric.dataagent.evaluation import evaluate_data_agent

data_agent_name  = "clinical_insights_agent"
workspace_name   = "Ryan Development Workspace"   # only needed if agent is in another workspace
table_name       = f"{data_agent_name}_evaluation_output"
data_agent_stage = "sandbox"   # or "production"

evaluation_id = evaluate_data_agent(
    df,
    data_agent_name,
    workspace_name=workspace_name,
    table_name=table_name,
    data_agent_stage=data_agent_stage,
)
print(f"evaluation_id = {evaluation_id}")
```

### 9.5 Inspect the results

```python
from fabric.dataagent.evaluation import get_evaluation_summary, get_evaluation_details

# High-level accuracy
get_evaluation_summary(table_name=table_name, verbose=True)

# Per-row failures with judge reasoning + thread_url
failures = get_evaluation_details(
    evaluation_id,
    table_name=table_name,
    get_all_rows=False,
    verbose=True,
)
display(failures)
```

For each False row:

1. Click `thread_url` to see the SQL the agent generated, the data returned, and the judge's reasoning.
2. Decide whether the failure is:
   - **An agent bug** → fix via better instructions / examples / descriptions.
   - **An ambiguous question** → tighten the question (e.g., add tiebreaker) and re-run.
   - **A judge false negative** (semantic match the judge missed) → relax with a custom `critic_prompt`, or accept-list both answers.

### 9.6 Iterate

Tune → re-run → inspect → repeat until accuracy on your ground truth crosses your acceptance threshold (typically 90%+ for "ready to publish").

---

## 10. Phase 7 — Publish to production

Once evaluation passes:

1. In the agent authoring canvas, click **Publish**.
2. Provide a **version label** and release notes (e.g., *"v1.0 — initial clinical insights agent, 95% accuracy on ground-truth set"*).
3. The agent now has two stages:
   - **sandbox** — your live editing version. Continues to evolve.
   - **production** — the immutable published version that downstream consumers call.

### 10.1 Re-run evaluation against `production`

```python
evaluation_id_prod = evaluate_data_agent(
    df, data_agent_name,
    workspace_name=workspace_name,
    table_name=table_name,
    data_agent_stage="production",
)
```

Promotion rule of thumb: don't promote a new sandbox version to production unless its eval accuracy ≥ the current production version's accuracy on the same ground truth.

---

## 11. Phase 8 — Consumption patterns

A published agent can be consumed in several ways. Pick what fits your audience.

### 11.1 Inside Fabric — chat experience

End users with workspace access can open the agent and chat with it directly. Good for analysts.

### 11.2 Embedded in Power BI via Copilot

Surface the agent as a **Copilot skill** so users can ask its questions from within reports.

### 11.3 Programmatic — via the SDK

For apps, automation, or other agents:

```python
from fabric.dataagent.client import FabricOpenAI

client = FabricOpenAI(
    artifact_name="clinical_insights_agent",
    workspace_name="Ryan Development Workspace",
    ai_skill_stage="production",
)

resp = client.chat.completions.create(
    model="not-used",
    messages=[{"role": "user", "content": "How many patients have clinical notes?"}],
)
print(resp.choices[0].message.content)
```

Authentication uses the caller's Azure AD identity (via `azure-identity` / `DefaultAzureCredential` when running outside Fabric).

### 11.4 REST API / multi-agent orchestration

The published agent also exposes a REST endpoint that other agents (e.g., a Foundry orchestrator) can call. Use this for cross-domain agents that need to combine answers from a clinical agent, a finance agent, etc.

---

## 12. Phase 9 — Monitoring and regression testing

A data agent is a living system. Schedule recurring checks.

### 12.1 Scheduled eval runs

Create a Fabric **scheduled notebook** (or pipeline) that:

1. Re-runs `evaluate_data_agent(...)` against the **production** stage on a cadence (daily or weekly).
2. Loads results from `*_evaluation_output` and computes the latest accuracy.
3. Alerts (Teams webhook, email, Activator rule) if accuracy drops below a threshold.

### 12.2 Trend the accuracy

Build a small Power BI report over the eval lakehouse:

- Accuracy by run date.
- Failure rate by question (which questions are flaky vs. consistently failing).
- Failure rate by capability (counts vs. window functions vs. text lookups).

A consistently failing question is a signal to either fix the agent or remove an ambiguous test.

### 12.3 Track schema drift

If upstream lakehouse tables change (new columns, renamed columns), the agent's grounding can silently degrade. Add a CI check that:

- Lists the current columns of agent-exposed tables/views.
- Compares against a checked-in schema snapshot.
- Fails the build if drift is detected without an accompanying agent update.

### 12.4 Capture real user questions

Periodically review the agent's thread history (in the agent UI). Real user questions reveal:

- Capabilities you didn't anticipate (add examples + ground truth).
- Common failure modes (tune instructions).
- Out-of-scope requests (refine guardrails).

Roll the best ones back into your ground-truth dataset.

---

## 13. Phase 10 — Governance, security, and cost

### 13.1 Identity and authorization

- The agent executes queries **on behalf of the caller's identity** (delegated auth), not a service principal. The caller must have read access to the underlying lakehouse.
- Use **lakehouse-level RBAC** to control who can query at all.
- Use **SQL row-level security (RLS)** / **column-level security (CLS)** on the SQL endpoint to filter results per user. The agent respects these because the query runs as the caller.

### 13.2 Data residency

If your capacity and the AI service region differ, you must explicitly enable **cross-geo processing/storing for AI** at the tenant level. Confirm with compliance before enabling for regulated data (e.g., PHI).

### 13.3 PII / PHI handling

- Expose **only the columns the agent needs** via curated views.
- Mask or hash sensitive columns at the view level (e.g., redact MRN unless caller is in a privileged role via RLS).
- Put guardrails in the agent's instructions ("Do not return raw memo content if asked to summarize across patients").
- Never put real PHI/PII into the **ground-truth dataset** — that file lives in source control. Use synthetic IDs and values.

### 13.4 Cost

Each agent invocation consumes Fabric capacity (LLM tokens + SQL execution). To control cost:

- Pre-materialize expensive joins/aggregations as views — cheaper than letting the agent rebuild them per question.
- Limit the agent to a small number of tables — fewer tokens spent on schema grounding.
- Cache common questions in your application layer when appropriate.

### 13.5 Versioning and source control

Although the agent itself is configured in the Fabric UI, version-control everything around it:

- Ground-truth CSV in the repo.
- Evaluation notebook in the repo (or in a Git-integrated workspace).
- The agent's instructions and example queries copied into a markdown doc in the repo so changes are reviewable.
- Schema snapshots of agent-exposed tables.

---

## 14. Troubleshooting cheat sheet

| Symptom | Likely cause | Fix |
|---|---|---|
| `pip install fabric-data-agent-sdk` returns *"No matching distribution"* | Python 3.13+ (SDK supports 3.10–3.12) | Use a Python 3.12 venv. |
| `Error getting default lakehouse path: Missing required Fabric context parameters` | Notebook has no default lakehouse attached | Attach a lakehouse (e.g., `lh_data_agent_eval`) and set it as default. |
| Eval reports False but the answer looks right | Format mismatch or LLM-judge false negative | Inspect `actual_answer` vs `expected_answer`; tighten formatting in the question, or relax with a custom `critic_prompt`. |
| Eval reports False because of a tie (e.g., two notes on same date) | Ambiguous "most recent" question | Add an explicit tiebreaker to the question and ensure the agent's instructions/examples encode the same tiebreaker. |
| Agent picks the wrong table | Too many tables exposed; weak descriptions | Reduce exposed tables; sharpen table/column descriptions. |
| Agent invents columns or values | Missing column descriptions or stale schema | Add descriptions; re-save agent to re-index. |
| Agent returns SQL instead of an answer | Instructions don't say "execute and answer" | Add to instructions: *"Always execute the query and return the result, not the SQL."* |
| Long free-text answers fail evaluation | Judge struggles with multi-paragraph matching | Replace with single-fact probes ("What is the MRN?") instead of full-memo matching. |
| `FutureWarning: 'type' parameter is deprecated` from sempy | SDK internal deprecation warning | Ignore — cosmetic only. |

---

## 15. References

- [Fabric Data Agent — overview](https://learn.microsoft.com/en-us/fabric/data-science/concept-data-agent)
- [Evaluate your data agent (SDK)](https://learn.microsoft.com/en-us/fabric/data-science/evaluate-data-agent)
- [`fabric-data-agent-sdk` on PyPI](https://pypi.org/project/fabric-data-agent-sdk/)
- [Microsoft Fabric samples — data agents](https://github.com/microsoft/fabric-samples)
- [Lakehouse SQL analytics endpoint](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)

---

## Appendix A — Reusable instructions template

The instructions block is the single highest-leverage configuration on a Fabric Data Agent. The template below is a battle-tested structure for **insights agents over a Lakehouse**. Copy it into a new agent and replace the `«placeholders»` with your specifics.

### Why this structure works

Every section has a job:

| Section | Purpose |
|---|---|
| **ROLE** | Tells the model what it *is* and what it's *for*. Anchors all later decisions. |
| **DATA SOURCE** | Acts as the schema "cheat sheet" the model consults when generating SQL. Reduces hallucinated columns. |
| **QUERY RULES** | Encodes dialect (T-SQL on the Lakehouse SQL endpoint), conventions (ordering, tiebreakers, result caps), and hard constraints (read-only). |
| **PRIVACY & SAFETY** | Guardrails for sensitive data, scope, and refusal behavior. |
| **RESPONSE STYLE** | Shapes output formatting so consumers (UI / Power BI / downstream agents) get consistent answers. |
| **FEW-SHOT EXAMPLES** | The most important section. Each NL→SQL pair teaches a query pattern. The model generalizes from these aggressively. |

### Template

````text
ROLE
You are an agent responsible for answering analytical questions about
«domain — e.g. clinical notes, sales orders, support incidents» from the
«workspace / lakehouse name» lakehouse. Use this agent for any question
about «list the in-scope entities and concepts».

DATA SOURCE
- Schema.table: «schema».«table_or_view»
- Columns:
    «col_1»   («type»)  -- «one-line description; for enums, list allowed values»
    «col_2»   («type»)  -- «...»
    «col_3»   («type»)  -- «...»
    «col_N»   («type»)  -- «...»

(Repeat the block above for each additional table/view the agent is allowed
to query. Keep this list as small as possible — fewer tables = better
table selection.)

QUERY RULES
1. Always emit T-SQL compatible with the Microsoft Fabric Lakehouse SQL
   analytics endpoint. Do NOT use features unsupported there
   (no temp tables, no MERGE, no stored procedures, no INSERT/UPDATE/DELETE,
   no CREATE/ALTER/DROP).
2. Reference tables as `«schema».«table»` (schema.table). Never prefix with
   a database/lakehouse name.
3. This is a READ-ONLY assistant. Refuse any request that would modify data
   or schema. Politely explain you can only run SELECT queries.
4. Always qualify columns when more than one table is referenced.
5. Use single quotes for string literals. Parameterize values inline
   (the engine does not accept `?` placeholders here).
6. When listing «primary entity» records, ORDER BY «default ordering column»
   «DESC|ASC» unless the user specifies otherwise.
7. When listing distinct «primary entity», use
   SELECT DISTINCT «entity_id» ... ORDER BY «entity_id».
8. For free-text search inside «text column», use `LIKE '%term%'` and make
   the match case-insensitive by wrapping both sides in LOWER().
9. For "most recent / latest per group" questions, always use a
   deterministic tiebreaker:
       ROW_NUMBER() OVER (PARTITION BY «group_col»
                          ORDER BY «date_col» DESC, «tiebreaker_col» ASC)
   Pick a tiebreaker column (e.g. primary key) that exists in the table.
10. Cap exploratory result sets with TOP 100 unless the user asks for more.
11. Format dates as YYYY-MM-DD in SQL literals and in output.
12. Handle nulls explicitly on categorical columns:
        ISNULL(«col», '(unspecified)')
13. Never invent columns, tables, or values. If a requested field does not
    exist in the schema above, say so and suggest the closest available
    column.

PRIVACY & SAFETY
- Treat all data as «sensitivity classification — e.g. PHI, PII, Confidential».
- Do not summarize, paraphrase, or repeat «sensitive_text_column» unless the
  user explicitly asks for that field.
- Do not «domain-specific forbidden inferences — e.g. provide medical advice,
  give legal opinions, make hiring recommendations». Stick to what the
  returned rows contain.
- If a user asks for data outside «schema».«table», respond that this agent
  is scoped to «in-scope domain» only.
- If the question requires data not present in the configured tables, say so
  explicitly rather than guessing.

RESPONSE STYLE
- Always execute the query and return the result. Show the T-SQL you ran,
  then a concise answer based on the result set.
- Prefer tables for multi-row results, bullet lists for short enumerations,
  and "key: value" pairs for per-group results.
- If a query returns zero rows, state that plainly and suggest a refinement
  (e.g. check «entity_id» spelling, broaden the date range).
- If the user's request is ambiguous (e.g. "recent X" without a date range),
  ask one clarifying question before querying.
- Use the conventions in QUERY RULES (date format, tiebreaker, null label)
  consistently in every response.

FEW-SHOT EXAMPLES
(Provide ~8–12 examples covering every SQL pattern the agent should support:
 simple count, scoped count, distinct list, top-N, grouped aggregation with
 ordering, date-range filter, text search, per-group latest with tiebreaker,
 join (if multiple tables), and any domain-specific pattern.)

Q: «Simple count question»
SQL:
SELECT COUNT(*) AS «alias»
FROM «schema».«table»;

Q: «Distinct list question»
SQL:
SELECT DISTINCT «entity_id»
FROM «schema».«table»
ORDER BY «entity_id»;

Q: «Top-N grouped aggregation»
SQL:
SELECT TOP 10 «group_col», COUNT(*) AS «alias»
FROM «schema».«table»
GROUP BY «group_col»
ORDER BY «alias» DESC;

Q: «Date-range filter»
SQL:
SELECT «cols»
FROM «schema».«table»
WHERE «date_col» >= 'YYYY-01-01'
  AND «date_col» <  'YYYY-01-01'
ORDER BY «date_col» DESC;

Q: «Free-text search»
SQL:
SELECT «cols»
FROM «schema».«table»
WHERE LOWER(«text_col») LIKE '%term%'
ORDER BY «date_col» DESC;

Q: «Grouped categorical with null handling»
SQL:
SELECT ISNULL(«cat_col», '(unspecified)') AS «cat_col»,
       COUNT(*) AS «alias»
FROM «schema».«table»
GROUP BY «cat_col»
ORDER BY «alias» DESC;

Q: «Most recent record per group, with deterministic tiebreaker»
SQL:
WITH ranked AS (
  SELECT «cols»,
         ROW_NUMBER() OVER (PARTITION BY «group_col»
                            ORDER BY «date_col» DESC, «pk_col» ASC) AS rn
  FROM «schema».«table»
)
SELECT «cols»
FROM ranked
WHERE rn = 1
ORDER BY «group_col»;
````

### Worked instantiation (clinical notes)

The clinical-notes instructions you developed are this template with the
placeholders filled in:

- `«domain»` → *clinical notes*
- `«schema».«table»` → `health_dbo.clinical_notes`
- `«entity_id»` → `patient_id`
- `«date_col»` → `note_date`
- `«tiebreaker_col»` / `«pk_col»` → `note_id`
- `«cat_col»` → `note_type`
- `«text_col»` / `«sensitive_text_column»` → `memo_content`
- `«sensitivity classification»` → *PHI*

### Tips for adapting the template

- **Keep the section order.** The model reads top-to-bottom; foundational context (ROLE, DATA SOURCE) needs to land before rules and examples.
- **Numbered rules > prose.** Models follow numbered, declarative rules more reliably than paragraphs.
- **One rule, one idea.** If a rule has an "and" in it, consider splitting it.
- **Examples > prose rules.** Whenever you find yourself writing a complex rule, ask: "Could I express this as one more example query instead?" The answer is usually yes, and examples generalize better.
- **Re-test after every instruction change.** Even small wording changes can shift behavior. Run your eval set (§9) after each meaningful edit.
- **Keep instructions under ~2,000 tokens.** Beyond that, attention to any one rule drops. Aggressive editing beats accretion.

---

## Appendix B — Quickstart checklist

```
[ ] Design
    [ ] One-sentence charter written
    [ ] Data surface inventoried (tables, grain, sensitivity)
    [ ] 10–20 expected questions listed and categorized
    [ ] Out-of-scope list written

[ ] Lakehouse prep
    [ ] Tables/columns have semantic names (or curated views)
    [ ] Table & column descriptions filled in
    [ ] Agent-facing views created (narrow column set)
    [ ] Data quality issues triaged (nulls, dupes, types)

[ ] Agent build
    [ ] Data agent created with naming convention
    [ ] Only the agent-facing tables/views exposed
    [ ] Instructions written (persona, rules, guardrails)
    [ ] 10+ example queries covering every SQL pattern

[ ] Evaluation
    [ ] Dedicated eval lakehouse created and attached
    [ ] Ground-truth CSV committed to repo
    [ ] evaluate_data_agent run; accuracy ≥ threshold
    [ ] All False rows triaged (agent fix vs. question fix vs. judge relax)

[ ] Publish & consume
    [ ] Published with version label and release notes
    [ ] Production-stage eval run and recorded
    [ ] Consumer integration tested (SDK / Copilot / REST)

[ ] Monitor
    [ ] Scheduled eval job set up
    [ ] Accuracy-trend report built
    [ ] Schema-drift check in CI
    [ ] Real-user-question review cadence agreed
```
