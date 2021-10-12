from . import APP

from utils.connection import connect_source
from . import PROJECT_ROOT
from utils.yaml import Yaml
from utils.upload_process import upload_blob
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from google.cloud import bigquery
from google.oauth2 import service_account

def test_validation():
    
    yaml = Yaml(source='Local', yaml_route='{}/yaml_config_files/sample.yml'.format(PROJECT_ROOT))
    yaml = yaml.get_yaml_decoded()
    # Getting batch
    batch = connect_source(
        expectation_suite_name=yaml.get('EXPECTATION_SUITE_NAME'), 
        ge_datasource=yaml.get('GE_DATASOURCE'),
        table=yaml.get('TABLE'))
    
    result = batch.expect_column_values_to_be_in_set(
        yaml.get('COLUMN_NAME'),
        min_value=0,
        result_format={'result_format': 'COMPLETE'},
        include_config=True)
    
    if (result['success'] == False):  
        valores_invalidos = result['result']['unexpected_list']
        
        gcp_credentials='{}/credentials/credentials.json'.format(PROJECT_ROOT)
        credentials = service_account.Credentials.from_service_account_file(gcp_credentials)
        project_id = 'testfalab'
        client = bigquery.Client(credentials=credentials, project=project_id)

        query_job = client.query("""
            SELECT key, {0} as valor_rechazado
            FROM testfalab_table.data_test
            WHERE {0} IN {1}""".format(yaml.get('COLUMN_NAME'), tuple(valores_invalidos)))

        results = query_job.result()
        result_dataframe = results.to_dataframe()
        result_dataframe['validacion'] = 'valor_numerico'
        result_dataframe=result_dataframe[['key', 'validacion', 'valor_rechazado']]
        result_dataframe.to_csv('rechazados.csv', index=False, sep='\t')

    upload_blob('bucket-test-92821', 'rechazados.csv', 'rechazados.csv')