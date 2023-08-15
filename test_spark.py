import os
import json
import ads
import ocifs
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from IPython import get_ipython

def prepare_command(command: dict) -> str:
    """Converts dictionary command to the string formatted commands."""
    return f"'{json.dumps(command)}'"


ads.set_auth("resource_principal")  # Supported values: resource_principal, api_key

fs = ocifs.OCIFileSystem()

directory_path = "sa-cat-data-neu@frsulmjrrb5y/raw_csv/"
file_list = fs.ls(directory_path)

get_ipython().run_line_magic('load_ext', 'dataflow.magics')
print('successfully loaded data flow magics')


compartment_id = os.environ.get("NB_SESSION_COMPARTMENT_OCID")
logs_bucket_uri = "oci://spark_log@frsulmjrrb5y/logs"
metastore_id = "<metastore_id>"
print(compartment_id)

command = prepare_command(
    {
        "compartmentId": compartment_id,
        "displayName": "TestDataFLowSessionFlexShapes",
        "language": "PYTHON",
        "sparkVersion": "3.2.1",
        "numExecutors": 1,
        "driverShape": "VM.Standard.E4.Flex",
        "executorShape": "VM.Standard.E4.Flex",
        "driverShapeConfig": {"ocpus": 2, "memoryInGBs": 32},
        "executorShapeConfig": {"ocpus": 2, "memoryInGBs": 32},
        "type": "SESSION",
        "logsBucketUri": logs_bucket_uri,
        "configuration": {
            "fs.oci.client.hostname": "https://frsulmjrrb5y.compat.objectstorage.eu-frankfurt-1.oraclecloud.com"
        },
    }
)

get_ipython().run_line_magic('create_session', '-l python -c $command')
print('spark session created')


# In[ ]:


get_ipython().run_cell_magic('spark', '', 'bucket_name = "sa-cat-data-neu"\nnamespace = "frsulmjrrb5y"\nfolder_name = "working"\nfolder = "curated"\ndico_data2=dict()\nfor i in [\'attestations_ref\',\'avenants_ref\',\'clients_ref\',\'clt_att_ref\',\'contrats_ref\',\'garanties_ref\'\n          ,\'infosmandataires_ref\',\'persimpliq_ref\']:\n    dico_data2[i] = spark.read.parquet(f"oci://{bucket_name}@{namespace}/{folder_name}/{folder}/{i}.parquet")\nprint(\'reading DONE\')')


# In[ ]:


get_ipython().run_cell_magic('spark', '', 'dico_data2[\'clients_ref\'].write.mode(\'overwrite\').parquet(f"oci://{bucket_name}@{namespace}/working/curated/client_ref_test_test1.parquet")\nprint(\'STORING DONE\')')