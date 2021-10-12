from expectation_scripts import APP_CONTEXT
from utils.loggin_messages import info_messages

def connect_source(expectation_suite_name, ge_datasource, table, overwrite_existing=True):
    try:
        APP_CONTEXT.create_expectation_suite(expectation_suite_name,
            overwrite_existing=overwrite_existing)
    except Exception as E:
        message = 'Datavalidations exist', E
        info_messages(auto=False, message=message)
    batch_kwargs = {
    "datasource": ge_datasource,
    # This is specifying the full path via the BigQuery project.dataset.table format
    "table": table
    }
    batch = APP_CONTEXT.get_batch(batch_kwargs=batch_kwargs, expectation_suite_name=expectation_suite_name)
    return batch