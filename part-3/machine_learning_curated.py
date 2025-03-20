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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1742439816781 = glueContext.create_dynamic_frame.from_catalog(database="stedi_toke_db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1742439816781")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1742439841740 = glueContext.create_dynamic_frame.from_catalog(database="stedi_toke_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1742439841740")

# Script generated for node SQL Query
SqlQuery10 = '''
select a.`email`, s.`serialNumber`, a.`timestamp`, a.`x`, a.`y`, a.`z` 
from accelerometer a 
join step s on s.`sensorReadingTime` = a.`timestamp`
'''
SQLQuery_node1742441393067 = sparkSqlQuery(glueContext, query = SqlQuery10, mapping = {"step":StepTrainerTrusted_node1742439816781, "accelerometer":AccelerometerTrusted_node1742439841740}, transformation_ctx = "SQLQuery_node1742441393067")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1742441393067, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1742438539556", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1742439189434 = glueContext.getSink(path="s3://stedi-toke-bucket/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1742439189434")
AmazonS3_node1742439189434.setCatalogInfo(catalogDatabase="stedi_toke_db",catalogTableName="machine_learning_curated")
AmazonS3_node1742439189434.setFormat("json")
AmazonS3_node1742439189434.writeFrame(SQLQuery_node1742441393067)
job.commit()