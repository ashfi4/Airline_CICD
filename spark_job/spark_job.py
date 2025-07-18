from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import argparse
import sys
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger=logging.getLogger(__name__)
class spark_job:
    def __init__(self):
        self.spark=SparkSession.builder\
                   .appName("Full Automation Project")\
                   .getOrCreate()
        logger.info("SparkSession initialized and class instantiated.")
    
    def pyspark_job(self,env,bq_project,bq_dataset,transformed_table,route_insights_table,origin_insights_table):
        input_path=f"gs://spark_ariflow_bucket_test/source_{env}"
        logger.info (f"Data Fetched from the source path successfully")
        df_flight=self.spark.read.csv(input_path,header=True,inferSchema=True)

        transformed_data=df_flight.withColumn("is_weekend",when(col('flight_day').isin('Sat','Sun'),lit(1)).otherwise(lit(0)))\
                        .withColumn("lead_time_category",when((col("purchase_lead")< 7),lit('Last-Minute'))
                                                         .when((col("purchase_lead")>=7) & (col("purchase_lead")<30),lit('Short-Term'))
                                                         .otherwise(lit('Long-Term')))\
                                                         .withColumn( "booking_success_rate", expr("booking_complete / num_passengers"))
        
        route_insights=transformed_data.groupBy('route').agg(count('*').alias('total_booking'),
                                                             avg('flight_duration').alias('avg_flight_duration'),
                                                             avg('length_of_stay').alias("avg_stay_time"))
        
        booking_origin=transformed_data.groupBy('booking_origin').agg(count('*').alias('total_booking'),
                                                                      avg('booking_success_rate').alias('success_rate'),
                                                                      avg('purchase_lead').alias('avg_purchase_lead'))
        logger.info("Data Succefully Transformed in different data frame")

        logger.info("Write Operation Started")

        transformed_data.write\
                    .format('bigquery')\
                    .option('table',f'{bq_project}:{bq_dataset}.{transformed_table}')\
                    .option('writeMethod','direct')\
                    .mode('overwrite')\
                    .save()
        logger.info("Data Written Succesfully to transformed_table under Flight_dev")

        route_insights.write\
                .format('bigquery')\
                .option('table',f'{bq_project}:{bq_dataset}.{route_insights_table}')\
                .option('writeMethod','direct')\
                .mode('overwrite')\
                .save()
        logger.info("Data Written Succesfully to route_insights_table under Flight_dev")

        booking_origin.write\
                .format('bigquery')\
                .option('table',f'{bq_project}:{bq_dataset}.{origin_insights_table}')\
                .option('writeMethod','direct')\
                .mode('overwrite')\
                .save()
        logger.info("Data Written Succesfully to origin_insights_table under Flight_dev")

        return
    def parsing_data(self):
        parser=argparse.ArgumentParser("Automation of Flight Data On Arrival")
        parser.add_argument('--env',required=True,help='Environment variable')
        parser.add_argument('--bq_project',required=True,help='Project_Id')
        parser.add_argument('--bq_dataset',required=True,help='BigQuery Dataset')
        parser.add_argument('--transformed_table',required=True,help='Table1')
        parser.add_argument('--route_insights_table',required=True,help='Table2')
        parser.add_argument('--origin_insights_table',required=True,help='table3')
        args=parser.parse_args()
        return{
            'env':args.env,
            'bq_project':args.bq_project,
            'bq_dataset':args.bq_dataset,
            'transformed_table':args.transformed_table,
            'route_insights_table':args.route_insights_table,
            'origin_insights_table':args.origin_insights_table
        }  

if __name__=="__main__":
    obj=spark_job()
    args=obj.parsing_data()
    obj.pyspark_job(
        env=args['env'],
        bq_project=args['bq_project'],
        bq_dataset=args['bq_dataset'],
        transformed_table=args['transformed_table'],
        route_insights_table=args['route_insights_table'],
        origin_insights_table=args['origin_insights_table']
    )
        
        

    

        