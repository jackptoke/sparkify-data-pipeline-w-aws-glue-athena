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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1742389890746 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-toke-bucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1742389890746")

# Script generated for node Customer Trusted
CustomerTrusted_node1742389854337 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-toke-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1742389854337")

# Script generated for node SQL Query
SqlQuery4 = '''
select a.user as email, a.timestamp, a.x, a.y, a.z 
from customer_trusted c
join accelerometer_landing a on a.user = c.email
where c.shareWithResearchAsOfDate > 0 and a.timestamp >= c.sharewithresearchasofdate
'''
SQLQuery_node1742400254579 = sparkSqlQuery(glueContext, query = SqlQuery4, mapping = {"customer_trusted":CustomerTrusted_node1742389854337, "accelerometer_landing":AccelerometerLanding_node1742389890746}, transformation_ctx = "SQLQuery_node1742400254579")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1742400254579, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1742385513145", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1742390110340 = glueContext.getSink(path="s3://stedi-toke-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1742390110340")
AmazonS3_node1742390110340.setCatalogInfo(catalogDatabase="stedi_toke_db",catalogTableName="accelerometer_trusted")
AmazonS3_node1742390110340.setFormat("json")
AmazonS3_node1742390110340.writeFrame(SQLQuery_node1742400254579)
job.commit()