# Compute configuration probe evidence

1. Run `nb_compute_configuration_probe` only through the validation path that submits the intended `executionData.computeConfiguration`. Do not run any medallion notebook.
2. Save the submitted request ID, job instance ID, requested configuration, and UTC start/end timestamps.
3. From the probe output, copy the single JSON object whose `record_type` is `fabric_spark_compute_configuration_probe`.
4. Compare `driver_memory`, `driver_cores`, `executor_memory`, `executor_cores`, and `executor_instances` with the submitted configuration. Also retain the application ID, dynamic-allocation fields, and default parallelism.
5. Pass only when every requested value matches the effective value. An accepted submission by itself is not proof.

Recommended evidence envelope:

```json
{
  "request_id": "<Fabric request ID>",
  "job_instance_id": "<Fabric job instance ID>",
  "started_at_utc": "<ISO-8601 timestamp>",
  "completed_at_utc": "<ISO-8601 timestamp>",
  "requested_compute_configuration": {},
  "effective_compute_configuration": {}
}
```
