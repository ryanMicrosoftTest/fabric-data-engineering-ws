import base64, json, fabric.functions as fn
udf = fn.UserDataFunctions()

@udf.connection(argName="lh", alias="onelakeSecurity")
@udf.function()
def get_patients_by_id(lh: fn.FabricLakehouseClient) -> dict:
    cur = lh.connectToSql().cursor()
    cur.execute("SELECT DISTINCT patient_id FROM onelake_security_lh_with_schemas.health_dbo.clinical_notes ORDER BY patient_id")
    return {"ids": [r[0] for r in cur.fetchall()]}

@udf.connection(argName="lh", 
alias="onelakeSecurity")
@udf.function()
def get_clinical_notes_by_patient_id(lh: 
fn.FabricLakehouseClient, patientId: str) -> list:
    cur = lh.connectToSql().cursor()
    cur.execute(
        "SELECT note_id FROM onelake_security_lh_with_schemas.health_dbo.clinical_notes "
        "WHERE patient_id = ? ORDER BY note_date",
        (patientId,),
    )
    return [r[0] for r in cur.fetchall()]

@udf.connection(argName="lh", 
alias="onelakeSecurity")
@udf.function()
def get_clinical_note_by_patient_id(lh: fn.FabricLakehouseClient, noteId: str) -> dict:
    cur = lh.connectToSql().cursor()
    cur.execute(
        "SELECT note_id, patient_id, note_date, note_type, memo_content "
        "FROM onelake_security_lh_with_schemas.health_dbo.clinical_notes WHERE note_id = ?",(noteId,),)
    row = cur.fetchone()
    if not row:
        raise fn.UserThrownError("note not found", {"noteId":noteId})
    note_obj = {"id": row[0], "text": row[4], "date":str(row[2]), "type": row[3] or "clinical note"}
    payload = base64.b64encode(json.dumps(note_obj).encode("utf-8")).decode("utf-8")
    return {
        "resourceType": "DocumentReference",
        "id": noteId,
        "subject": {"reference": f"Patient/{row[1]}"},
        "content": [{"attachment": {"contentType":
"application/json", "data": payload}}],
    }