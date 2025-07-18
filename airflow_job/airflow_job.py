from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from datetime import datetime,timedelta
import uuid

class build_dag:
    def __init__(self):
        self.default_args={
            'owner':'airflow',
            'depend_on_past':False,
            'email_on_failure':False,
            'email_on_retry':False,
            'retries':1,
            'retry_delay':timedelta(minutes=5),
            'start_date':datetime(2025,7,18),
        }
        self.env=Variable.get('env',default_var='dev')
        self.bq_project=Variable.get('bq_project',default_var='my-project-68545-465117')
        self.gcs_bucket=Variable.get('gcs_bucket',default_var='spark_ariflow_bucket_test')
        self.bq_dataset=Variable.get('bq_dataset',default_var=f'flight_data_{self.env}')
        tables=Variable.get('tables',deserialize_json=True)
        self.transformed_table=tables['transformed_table']
        self.route_insights_table=tables['route_insights_table']
        self.origin_insights_table=tables['origin_insights_table']
        self.batch_id=f'batch-id-{str(uuid.uuid4())[:8]}'

    def spark_dag(self):
        with DAG(
            dag_id='Flight_DATA_Automation_process',
            default_args=self.default_args,
            description="Full Automation using GIT",
            schedule_interval=None,
            catchup=False,
            tags=['dev'],
        )as dag:
            sensor_task=self.sensor(dag)
            batch_config=self.batch_details()
            batch_server=self.batch_server(dag,batch_config)

            sensor_task >> batch_server

        return dag
    
    def sensor(self,dag):
        file_sensor=GCSObjectExistenceSensor(
            task_id='File_sensor',
            bucket=self.gcs_bucket,
            object=f'source_{self.env}/flight_booking.csv',
            google_cloud_conn_id="google_cloud_default",
            timeout=300,
            poke_interval=30,
            mode='poke',
            dag=dag
        )
        return file_sensor
    def batch_details(self):
        batch={
            'pyspark_batch':{
                'main_python_file_uri':'gs://spark_ariflow_bucket_test/spark_job/spark_job.py',
                'python_file_uris':[],
                'jar_file_uris':[],
                'args':[
                    f'--env={self.env}',
                    f'--bq_project={self.bq_project}',
                    f'--bq_dataset={self.bq_dataset}',
                    f'--transformed_table={self.transformed_table}',
                    f'--route_insights_table={self.route_insights_table}',
                    f'--origin_insights_table={self.origin_insights_table}'

                ]
            },
            'runtime_config':{
                'version': '2.2',
            },

            'environment_config':{
                'execution_config':{
                    'service_account':'113826229693-compute@developer.gserviceaccount.com',
                    'network_uri':'projects/my-project-68545-465117/global/networks/default',
                    'subnetwork_uri':"projects/my-project-68545-465117/regions/us-central1/subnetworks/default",
                }
            },
        }
        return batch
    def batch_server(self,dag,batch):
        server=DataprocCreateBatchOperator(
            task_id='Serverless_Batch_computation',
            batch=batch,
            batch_id=self.batch_id,
            project_id='my-project-68545-465117',
            region='us-central1',
            gcp_conn_id='google_cloud_default',
            dag=dag
        )
        return server
dag=build_dag().spark_dag()
        


    

        




