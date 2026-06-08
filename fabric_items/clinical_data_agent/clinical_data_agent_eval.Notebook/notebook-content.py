# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "65aa914a-be05-4b4d-86d4-da5dcabe035b",
# META       "default_lakehouse_name": "data_agent_evaluation_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "65aa914a-be05-4b4d-86d4-da5dcabe035b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install -U fabric-data-agent-sdk

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import pandas as pd
from fabric.dataagent.evaluation import evaluate_data_agent
from fabric.dataagent.evaluation import get_evaluation_summary
from fabric.dataagent.evaluation import get_evaluation_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Load Ground Truth Dataset

# CELL ********************

# Define a sample evaluation set with user questions and their expected answers.
# You can modify the question/answer paits to match your scenario
df = pd.DataFrame(
    columns=['question', 'expected_answer'],
    data=[
        ["List all patient IDs that have clinical notes.", "patient_4, patient_5, patient_6, patient_7"],
        ["How many patients have notes?", "4"],
        ["How many patient notes does patient_4 have", "16"],
        ["Give me the full memo for the most recent memo for patient_5", "Patient Name: Elizabeth Johnson\nMRN: 123456\nProvider: Dr. John Smith, Dr. Sarah Lin, Dr. Michael Brown\n\nSummary: Elizabeth Johnson remains disease-free 11 months post-surgery. Adjuvant osimertinib well-tolerated with minor manageable side effects.\n\nDiscussion: Imaging shows no evidence of recurrence. Lung function stable.\n\nPlan: Continue osimertinib for total duration of 3 years. Regular imaging every 6 months.\n\nConsensus: Continue current management strategy.\n\n[SYNTHETIC DATA — generated for HAO demo]"],
        ["Give me the full memo for the most recent patient note for patient_5 that mention 'hypertension'", "Patient Name: Elizabeth Johnson\nMRN: 123456\nProvider: Dr. John Smith\n\nStatus Post-Lobectomy: Patient recovering well. No signs of infection or complications.\n\nCurrent Medications: Osimertinib 80 mg daily, lisinopril 10 mg daily for hypertension.\n\nAssessment: No recurrence noted on follow-up imaging. Stable condition.\n\nPlan: Continue osimertinib. Regular follow-up every 3 months with imaging to monitor for recurrence.\n\n[SYNTHETIC DATA — generated for HAO demo]"],
        ["What note types exist in clinical_notes and how common is each? Order from most to least common.","Progress Notes: 12, Multidisciplinary Tumor Board Discussion: 6, Telephone Encounter: 5, Pathology and Cytology: 4, Lab Results: 3, Initial Consult: 3, Patient Instructions: 3, Radiology Report: 3, Surgical Operative Note: 2, Treatment Plan: 2, Procedures: 1, CT: 1, PET: 1"],
        ["For each patient, what is the date of their most recent clinical note? List as 'patient_id: date' pairs.", "patient_4: 2021-03-27, patient_5: 2025-03-12, patient_6: 2025-02-01, patient_7: 2024-11-05"],
        ["For each patient, what is the note_type of their most recent clinical note? Break ties by note_id ascending. List as 'patient_id: note_type' pairs.","patient_4: Progress Notes, patient_5: Multidisciplinary Tumor Board Discussion, patient_6: Multidisciplinary Tumor Board Discussion, patient_7: Multidisciplinary Tumor Board Discussion"]
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Evaluate and Assess Data Agent

# CELL ********************

# Name of your Data Agent
data_agent_name = "clinical_data_agent"

# Name of the workspace
workspace_name = "Ryan Development Workspace"
table_name = f"{data_agent_name}_evaluation_output"

# Specify the Data Agent stage: "production" (default) or "sandbox"
data_agent_stage = "production"

# Run the evaluation and get the evaluation ID
evaluation_id = evaluate_data_agent(
    df,
    data_agent_name,
    workspace_name=workspace_name,
    table_name=table_name,
    data_agent_stage=data_agent_stage
)

print(f"Unique ID for the current evaluation run: {evaluation_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Get Evaluation Summary

# CELL ********************

# Retrieve a summary of the evaluation results
eval_df = get_evaluation_summary(table_name)

eval_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Inspect Detailed Evaluation Results

# CELL ********************

# Whether to return all evaluation rows (True) or only failures (False)
get_all_rows = False

# Whether to print a summary of the results
verbose = True

# Retrieve evaluation details for a specific run
eval_details = get_evaluation_details(
    evaluation_id,
    table_name,
    get_all_rows=get_all_rows,
    verbose=verbose
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
