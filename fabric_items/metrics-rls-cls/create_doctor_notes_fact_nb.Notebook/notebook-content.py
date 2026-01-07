# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Add Additional Records to doctor notes fact table

# CELL ********************

# parameters
workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
lakehouse_id = '5090acaa-9246-4eb2-b9fc-93b8b6e7e60c'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, udf, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql import Row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Setup Doctor Dim Table

# CELL ********************

doctor_df = spark.read.load(f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/deidentified_dr_notes')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(doctor_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# select the dim_doctor

dim_doctor_df = spark.read.load(f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/dim_doctor')

display(dim_doctor_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# add doctor column to doctor_df

doctor_df = doctor_df.drop('doctor','id')

# rename the id column in dim_doctor_df to doctor_id to make the join easier
dim_doctor_df = dim_doctor_df.withColumnRenamed('id', 'doctor_id')

doctor_df = doctor_df.join(dim_doctor_df, on='doctor_id', how='left')

# drop the upn column
doctor_df = doctor_df.drop('upn')

display(doctor_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Add Department Dimension

# CELL ********************


department_df = spark.createDataFrame([
    {
        'department_id': 1,
        'department_name': 'Cardiology'
    },
    {
        'department_id': 2,
        'department_name': 'Neurology'
    },
    {
        'department_id': 3,
        'department_name': 'Orthopedics'
    },
    {
        'department_id': 4,
        'department_name': 'Dermatology'
    },
    {
        'department_id': 5,
        'department_name': 'Gastroenterology'
    }
])


display(department_df)

department_df.write.format('delta').mode('overwrite').save(f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/dim_department')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Map existing fact records to doctor and department

# CELL ********************

department_map = {
    1: "Cardiology",
    2: "Neurology",
    3: "Orthopedics",
    4: "Dermatology",
    5: "Gastroenterology",
    6: "Cardiology",
    7: "Neurology",
    8: "Orthopedics",
    9: "Gastroenterology"
}
department_dim_df = doctor_df

# remove doctor and id if present
try:
    department_dim_df = department_dim_df.drop('id')
except:
    print('values already removed')

# add department column and mapping udf
get_department = udf(lambda id: department_map.get(id), StringType())
department_dim_df = department_dim_df.withColumn('department', get_department('doctor_id'))

# drop all columns except doctor and department
department_dim_df = department_dim_df.drop('doctor_id', 'file_path', 'memo_content', 'memo_id')

# drop any where doctor is NULL
department_dim_df = department_dim_df.where('doctor IS NOT NULL')

# append Dr. Harley Muir, MD if not present in dim_department
dr_muir_exists = department_dim_df.filter(department_dim_df['doctor']=='Dr. Harley Muir, MD').count() > 0

if not dr_muir_exists:
    # append
    new_row = spark.createDataFrame([Row(doctor='Dr. Harley Muir, MD',department='Gastroenterology')])
    department_dim_df = department_dim_df.union(new_row)

display(department_dim_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

department_dim_df.write.format('delta').option("overwriteSchema", "true").mode('overwrite').save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Tables/dim_department')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write as obt fact table
obt_fact = spark.read.format('delta').load('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Tables/doctor_memos_fact_obt')

# need to add department to this as t his is what will be used to filter
obt_fact = obt_fact.join(department_dim_df, how='left', on='doctor')

display(obt_fact)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write as updated obt_fact
obt_fact.write.format('delta').mode('overwrite').option('mergeSchema','true').save('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Tables/doctor_memos_fact_obt')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# join the two pyspark dataframes
# doctor_table_df = doctor_table_df.withColumnRenamed('id', 'doctor_id')
# doctor_df = doctor_df.withColumnRenamed('id', 'memo_id')

# df_joined = doctor_df.join(doctor_table_df, on='doctor', how='outer')

# display(df_joined)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# doctor_df = doctor_df.drop('id', 'doctor')

# doctor_df = doctor_df.select('memo_id', 'memo_content', 'doctor_id', 'file_path')
# doctor_df = doctor_df.orderBy('memo_id')
# display(doctor_df)

# doctor_df.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/deidentified_dr_notes')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Name this section

# CELL ********************

# drop id col and change order to memo_id, memo_content, doctor_id, file_path

df = df.drop('id')
df = df.select('memo_id', 'memo_content', 'doctor_id')

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

doctor_df = spark.read.load('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Tables/dim_doctor')
doctor_df.select('doctor')

display(doctor_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# ---- Settings
target_table = "doctor_memo_fact"

# ---- Allowed doctors (exactly as provided)
doctors = [
    ("Dr. Antionette Klerkx, MD", 1),
    ("Dr. Harley Muir, MD", 2),
    ("Dr. Jean Urbonas, MD", 3),
    ("Dr. Kwame M’Meeuwis, MD", 4),
    ("Dr. Monroe Koster, MD", 5),
    ("Dr. Perla Swartz, MD", 6),
    ("Dr. Stephany Quixada, MD", 7),
    ("Dr. Trista Beltran, MD", 8),
    ("Dr. Wesson Arkwright, MD", 9),
]

# Small pools to create realistic variety (deterministic, no external randomness)
specialties = [
    ("General Medicine", "Bronchitis Follow-up"),
    ("Cardiology", "Hypertension Check"),
    ("Endocrinology", "Diabetes Management"),
    ("Pulmonology", "Asthma Follow-up"),
    ("Dermatology", "Rash Evaluation"),
    ("Neurology", "Migraine Follow-up"),
    ("Gastroenterology", "GERD Management"),
    ("Orthopedics", "Knee Pain Evaluation"),
]
hospitals = [
    "Keck Hospital", "Harborview Medical Center", "Mercy General Hospital",
    "St. Raphael Clinic", "Cedar Grove Hospital", "Riverside Medical Pavilion"
]
cities_states = [
    ("Wonder Lake", "SD"), ("Burlingame", "CA"), ("Dover", "DE"),
    ("Raleigh", "NC"), ("Madison", "WI"), ("Salem", "OR"),
    ("Boulder", "CO"), ("Ann Arbor", "MI"), ("Reno", "NV"), ("Gainesville", "FL")
]
streets = ["Holladay Ln", "Maple Ave", "Pine St", "Elm Dr", "Cedar Ct", "Birch Way", "Oak Blvd", "Willow Rd"]
professions = ["Engineer", "Teacher", "Nurse", "Analyst", "Designer", "Technician", "Developer", "Manager"]
orgs = ["Seaside Market", "Northwind Health", "Contoso Builders", "Fabrikam Foods", "Proseware Labs", "Tailspin Toys"]
first_names = ["Willard", "Avery", "Samuel", "Nora", "Elena", "Marcus", "Iris", "Jamal", "Priya", "Diego"]
last_names = ["Nardi", "Chen", "Ortiz", "Patel", "Santos", "Klein", "Ahmed", "Hughes", "Garcia", "Kim"]
locations_other = [
    "McKenzie Pass (patient reports brisk walking here daily)",
    "Greenbelt Park (light daily jogs)",
    "Riverside Trail (evening strolls)",
    "Downtown Loop (commuter walking)",
]

assessments = [
    "Persistent cough improving; CXR consistent with bronchitis. Continue albuterol via nebulizer, azithromycin day {d_mod}/5.",
    "BP trending down; continue lisinopril; low-sodium diet reinforced; follow-up BMP in 2 weeks.",
    "A1c stable; continue metformin; reinforce diet & exercise; CGM review next visit.",
    "Asthma stable on current regimen; refill inhaler; spacer technique reviewed.",
    "Topical steroid started for rash; avoid irritants; recheck in 10 days.",
    "Migraine frequency reduced; continue prophylaxis; maintain trigger diary.",
    "Symptoms of GERD improved; continue PPI; lifestyle counseling provided.",
    "Knee pain likely overuse; RICE, PT referral; NSAIDs PRN; imaging if persists.",
]

# Helper to build deterministic, varied yet synthetic memo content
def build_memo(memo_id: int, doctor_name: str, doctor_id: int):
    # Deterministic indexers
    idx = memo_id - 10
    spec_name, spec_reason = specialties[idx % len(specialties)]
    hospital = hospitals[idx % len(hospitals)]
    city, state = cities_states[idx % len(cities_states)]
    street = streets[idx % len(streets)]
    profession = professions[idx % len(professions)]
    org = orgs[idx % len(orgs)]
    first = first_names[idx % len(first_names)]
    last = last_names[idx % len(last_names)]
    nickname = first.split()[0]  # simple nickname from first name
    d_mod = (idx % 5) + 1

    # Dates & simple numeric fields
    visit_date = (date(2025, 9, 16) + timedelta(days=idx)).isoformat()
    house_no = 900 + idx + 33
    zip_code = 61000 + 800 + idx + 71  # just to look "zip-like"
    ip_last_octet = 100 + idx  # 203.0.113.X is TEST-NET-3 (safe)
    vin_suffix = f"{760900 + idx:06d}"
    plate_suffix = f"{(300 + idx):03d}"
    mrn = f"W-1699-47{300 + idx:03d}"
    hmo = f"NN-4345-44{70 + idx:02d}"
    acct = f"NM-7669627{100 + idx:03d}"
    ssn = f"307-61-25{60 + idx:02d}"  # synthetic-looking
    passport = f"W{31789900 + idx:02d}"
    driver = f"SU N7962{3800 + idx:04d}"
    neb = f"SN-NB-71F8-{110 + idx:03d}"
    watch = f"DH-7ZI9-{(50 + idx):04d}"
    bioid = f"ZV-{2326 + idx}"
    vin = f"9GQRT13349I{vin_suffix}"
    plate = f"SU 3ZM-J{plate_suffix}"
    token = f"YN-585{idx}-UNCL"
    phone_mid = f"{9600 + idx:04d}"
    phone_end = f"{8600 + idx:04d}"

    # Contact (synthetic, safe)
    doctor_email_local = doctor_name.lower().replace(" ", ".").replace("’", "").replace("'", "").replace(",", "").replace("—", "").replace("–", "").replace(".", "")
    doctor_email_local = doctor_email_local.replace("md", "").strip(".")
    doctor_email = f"{doctor_email_local}@hospital.example" if doctor_email_local else f"doctor{doctor_id}@hospital.example"

    patient_email = f"{first.lower()}.{last.lower()}@example.com"
    username = f"{first[0].lower()}{last.lower()}{(40 + idx)}"

    # Address
    address_line = f"{house_no} {street}"

    # Assessment selection
    assessment = assessments[idx % len(assessments)].format(d_mod=d_mod)

    memo = (
f"Memo {memo_id} — {spec_name} ({spec_reason})\n"
f"Date: {visit_date}\n"
f"Hospital: {hospital} (Outpatient Clinic)\n"
f"Doctor: {doctor_name} — License: QY-APC-829189; Email: {doctor_email}; Fax: (108) 5435-9606\n"
f"Patient: {first} “{nickname}” {last} — Age: {43 + (idx % 12)} — Profession: {profession} Organization: {org}\n"
f"Address: {address_line}, City: {city}, State: {state}, Zip: {zip_code}, CountryOrRegion: United States\n"
f"Phone: (108) 9{phone_mid}-{phone_end} — Email: {patient_email} — Username: {username}\n"
f"MedicalRecord: MRN {mrn} — HealthPlan: Bright Health HMO ID {hmo} — Account: {acct}\n"
f"Government/Other IDs: SocialSecurity: {ssn}; IDNum (Passport): MetroPlus Health {passport}; License (Driver): {driver}\n"
f"Telehealth metadata: IPAddress: 203.0.113.{ip_last_octet} — Url (portal): https://portal{10 + idx}.example.org/ejwffz/T-4356-{47994 + idx}\n"
f"Devices: Home nebulizer Device OB-ZC-97B8-9{80 + idx}; Apple Watch Device S/P {watch}\n"
f"Biometric: BioID retinal scan ID {bioid} captured for secure sign-in\n"
f"Vehicle: {2010 + (idx % 15)} Horizon Academy — VIN: {vin} — Plate: {plate}\n"
f"LocationOther: {locations_other[idx % len(locations_other)]}\n"
f"Unknown: Internal Note Token: {token}\n"
f"Assessment/Plan: {assessment}"
    )

    return memo

# ---- Create rows for memo_id 10..34 (inclusive)
rows = []
for memo_id in range(10, 35):  # 25 memos
    doctor_name, doctor_id = doctors[(memo_id - 10) % len(doctors)]
    memo_content = build_memo(memo_id, doctor_name, doctor_id)
    rows.append(Row(memo_id=int(memo_id), memo_content=memo_content, doctor_id=int(doctor_id)))

# ---- Create DataFrame with the exact target schema/order
schema = StructType([
    StructField("memo_id", IntegerType(), nullable=False),
    StructField("memo_content", StringType(), nullable=False),
    StructField("doctor_id", IntegerType(), nullable=False),
])
memos_df = spark.createDataFrame(rows, schema=schema)

# (Optional) Preview before inserting
# display(memos_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(memos_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# outer join between two dataframes df and memos_df
joined_df = df.union(memos_df)
joined_df = joined_df.orderBy('memo_id')

display(joined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write to new table names doctor_memos_fact
# joined_df.write.format('delta').mode('overwrite').save(f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/doctor_memos_fact')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# update the doctor dim table to include user_principal_name column
doctor_dim_df = spark.read.load(f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/dim_doctor')

display(doctor_dim_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

user_principal_name_mapping = {
    1:'dr_klerkx@MngEnvMCAP372892.onmicrosoft.com',
    2: 'dr_muir@MngEnvMCAP372892.onmicrosoft.com'
}

get_doctor_upn = udf(lambda id: user_principal_name_mapping.get(id), StringType())

doctor_dim_df = doctor_dim_df.withColumn('upn', get_doctor_upn('id'))

display(doctor_dim_df)

# doctor_dim_df.write.mode('overwrite').option('mergeSchema','true').format('delta').save(f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/dim_doctor')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create one big table type fact table so that upn can be used to filter on

doctor_memo_fact_df = spark.read.load(f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/doctor_memos_fact')

joined_df = doctor_dim_df.join(doctor_memo_fact_df, doctor_memo_fact_df['doctor_id']==doctor_dim_df['id'], how='inner')
joined_df = joined_df.select('doctor', 'memo_content', 'upn')


# joined_df = joined_df.write.mode('overwrite').format('delta').save('abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/doctor_memos_fact_obt')
display(joined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
