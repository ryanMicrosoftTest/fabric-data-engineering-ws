from .engineapi.typedefinitions import GetScriptMessageArguments

def resolve_dataflow(dataflow: 'azureml.dataprep.Dataflow', span_context: 'DPrepSpanContext' = None) -> str:
    if dataflow._rs_dataflow_yaml is not None:
        return dataflow._rs_dataflow_yaml
    activity_data = dataflow._dataflow_to_anonymous_activity_data(dataflow)
    return dataflow._engine_api.get_script(GetScriptMessageArguments(anonymous_activity=activity_data, span_context=span_context))
