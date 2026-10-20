# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Define Data Masking on Student Records
# Objectives
# -   All data in Student table must have social security number masked <br>
# -   All data in employee table that is of a student must have social security number masked

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read from silver lakehouse
silver_lh_abfss_path = 'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/70e18f53-f14f-41bd-b3d0-8060d42c4909/Tables'

employee_df = spark.read.format('delta').load(f'{silver_lh_abfss_path}/employee')
student_df = spark.read.format('delta').load(f'{silver_lh_abfss_path}/student')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(employee_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(student_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load and Mask Bronze to Silver

# CELL ********************

# read from bronze lakehouse
bronze_lh_abfss_path = 'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/d550d915-f3a8-418b-b3f4-c2cb369838c3/Tables'

employee_df = spark.read.format('delta').load(f'{bronze_lh_abfss_path}/employee')
student_df = spark.read.format('delta').load(f'{bronze_lh_abfss_path}/student')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(employee_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(student_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# identify the records in employee that need to be masked

# rename the columns in advance to be easier to see
employee_df = (employee_df.withColumnRenamed('id', 'employee_id')
                          .withColumnRenamed('first_name', 'employee_first_name')
                          .withColumnRenamed('last_name', 'employee_last_name')
)

student_df = (student_df.withColumnRenamed('id','student_id')
                        .withColumnRenamed('first_name', 'student_first_name')
                        .withColumnRenamed('last_name', 'student_last_name')

)

student_employees_df = employee_df.join(student_df, on='social_security_number', how='inner')

display(student_employees_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get the employee id's that must be masked
emp_id_mask_list = student_employees_df.select('employee_id').collect()

emp_id_mask_list = [row['employee_id'] for row in emp_id_mask_list]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_id_mask_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df = employee_df.filter(employee_df['employee_id'].isin(emp_id_mask_list))

# mask the social security number to XXX-XX-last-4-digits

display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee_df = employee_df.withColumn(
    "social_security_number",
    F.when(
        F.col("employee_id").isin(emp_id_mask_list),
        F.concat(
            F.lit("XXX-XX-"),
            F.substring(F.col("social_security_number"), -4, 4)
        )
    ).otherwise(F.col("social_security_number"))
)

display(employee_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# option B -> Don't see records if student

no_student_df = employee_df.filter(~F.col("id").isin(emp_id_mask_list))

display(no_student_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# option C -> Don't see column at all

no_soc_sec_num_df = employee_df.drop('social_security_number')

display(no_soc_sec_num_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
