import time
from . import app, context
from flask import request, jsonify
from google.cloud import bigquery
from google.oauth2 import service_account
from src.utils.upload_process import upload_blob
from src.utils.utils import get_project_path
from src.utils import batch_validations
from werkzeug.exceptions import NotFound


@app.route('/validation_column_between', methods=['GET'])
def validation_column_between():
    """Api to validate Bigquery fields 
    """
    # Getting parameters
    parameters = request.json
    if not parameters:
        return 'body not founded'

    response = []

    batch = get_batch_file(
        expectation_suite_name = parameters.get('EXPECTATION_SUITE_NAME'),
        ge_datasource=parameters.get('GE_DATASOURCE'),
        table=parameters.get('TABLE'),
        )

    columns = parameters.get('COLUMNS')
    for column_name in columns:
        # Creating results of expectations
        result = batch_validations.column_between(batch=batch, 
            column_name=column_name, 
            min_value=parameters.get('MIN_VALUE'))

        if (result['success'] == False):
            invalid_values = result['result']['unexpected_list']
            response.append(create_expectation_file(
                column_name=column_name,
                gcp_project=parameters.get('GCP_PROJECT'),
                table_id=parameters.get('TABLE_ID'),
                dataset=parameters.get('DATASET'),
                table=parameters.get('TABLE'),
                target_bucket=parameters.get('TARGET_BUCKET'),
                invalid_values=invalid_values),
                )
        else: 
            response.append(f"Validation pass correctly {column_name}")

    return jsonify(response)


@app.route('/validation_not_be_null', methods=['GET'])
def validation_not_be_null():
    """Api to validate Bigquery fields 
    """
    # Getting parameters
    parameters = request.json
    if not parameters:
        return 'body not founded'

    response = []

    batch = get_batch_file(
        expectation_suite_name = parameters.get('EXPECTATION_SUITE_NAME'),
        ge_datasource=parameters.get('GE_DATASOURCE'),
        table=parameters.get('TABLE'),
        )

    columns = parameters.get('COLUMNS')
    for column_name in columns:

        # Creating results of expectations
        result = batch_validations.column_not_be_null(batch=batch, 
            column_name=column_name)

        if (result['success'] == False):
            invalid_values = result['result']['unexpected_list']
            response.append(create_expectation_file(
                column_name=column_name,
                gcp_project=parameters.get('GCP_PROJECT'),
                table_id=parameters.get('TABLE_ID'),
                dataset=parameters.get('DATASET'),
                table=parameters.get('TABLE'),
                target_bucket=parameters.get('TARGET_BUCKET'),
                invalid_values=invalid_values),
                )
        else: 
            response.append(f"Validation pass correctly {column_name}")

    return jsonify(response)



def get_batch_file(expectation_suite_name, ge_datasource, table) -> str:
    """Get batch DF from datasource Great expectation

    Args:
        expectation_suite_name (str): expectation suite name from Great expectation config
        ge_datasource (str): Great expectations datasource config
        table (str): table name in Google Big Query

    Returns:
        Object: batch object
    """
    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
    batch_kwargs = {
        "datasource": ge_datasource,
        # This is specifying the full path via the BigQuery project.dataset.table format
        "table": table
    }

    # Creating batch to validate Dataset
    batch = context.get_batch(batch_kwargs=batch_kwargs, expectation_suite_name=expectation_suite_name)

    return batch

def create_expectation_file(invalid_values, table_id, table,
                            column_name, dataset, gcp_project, 
                            target_bucket) -> str:
    """Create query result file based in result of expectation

    Args:
        invalid_values (tuple): tuple with invalid values of expectation
        table_id (str): id of table
        table (str): table name
        column_name (str): Column name to validate
        dataset (str): Name of dataset in Big query
        gcp_project (str): GCP p[roject (validate the service account have permissions)
        target_bucket (str): Bucket name to save file

    Returns:
        str: name of file and bucket saved.
    """
    try: 
        credentials = service_account.Credentials.from_service_account_file(f'{get_project_path()}/credentials/credentials.json')
        project_id = gcp_project
        client = bigquery.Client(credentials=credentials, project=project_id)
        value_rejected = "value_rejected"
        query = f"""
            SELECT {table_id}, {column_name} as {value_rejected}
            FROM {dataset}.{table}
            WHERE {column_name} IN {tuple(invalid_values)}
        """
        query_job = client.query(query)

        results = query_job.result()
        result_dataframe = results.to_dataframe()
        dataTypeDict = dict(result_dataframe.dtypes)
        result_dataframe['validation_type'] = dataTypeDict.get(value_rejected)
        result_dataframe=result_dataframe[[table_id, 'validation_type', value_rejected]]

        ### Creating local file csv
        timestamp_validation = time.time_ns() // 1000000 
        file_name = f"{column_name}_{timestamp_validation}_rechazados.csv"
        source_file_name = f"{get_project_path()}/results/{file_name}"
        result_dataframe.to_csv(source_file_name, index=False, sep='\t')

        ### Up Blob to GCS
        response = upload_blob(
            bucket_name=target_bucket,
            source_file_name=source_file_name,
            destination_blob_name=file_name,
            gcp_project=gcp_project
        )
    except Exception as e:
        raise NotFound(e) 
    
    return response