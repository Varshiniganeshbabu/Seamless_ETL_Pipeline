# AWS Glue ETL job example
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load raw data from S3
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://student-data-2025/orders.csv"]},
    format="csv",
    format_options={"withHeader": True}
)

# Transformations (example: drop nulls, remove duplicates)
datasource1 = datasource0.drop_fields(['unnecessary_column']).drop_duplicates()

# Write processed data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=datasource1,
    connection_type="s3",
    connection_options={"path": "s3://target-data-student/"},
    format="csv"
)

job.commit()
