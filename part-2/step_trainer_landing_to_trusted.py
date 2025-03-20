import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1742399176993 = glueContext.create_dynamic_frame.from_catalog(database="stedi_toke_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1742399176993")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1742397441869 = glueContext.create_dynamic_frame.from_catalog(database="stedi_toke_db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1742397441869")

# Script generated for node SQL Query
SqlQuery5 = '''
select st.serialnumber AS serialNumber, 
sensorReadingTime, 
distanceFromObject 
from step_trainer_source st
join customer_source c on c.serialnumber = st.serialnumber
'''
SQLQuery_node1742398723583 = sparkSqlQuery(glueContext, query = SqlQuery5, mapping = {"step_trainer_source":StepTrainerLanding_node1742397441869, "customer_source":CustomerTrusted_node1742399176993}, transformation_ctx = "SQLQuery_node1742398723583")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1742398723583, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1742385513145", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1742393155264 = glueContext.getSink(path="s3://stedi-toke-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1742393155264")
AmazonS3_node1742393155264.setCatalogInfo(catalogDatabase="stedi_toke_db",catalogTableName="step_trainer_trusted")
AmazonS3_node1742393155264.setFormat("json")
AmazonS3_node1742393155264.writeFrame(SQLQuery_node1742398723583)
job.commit()