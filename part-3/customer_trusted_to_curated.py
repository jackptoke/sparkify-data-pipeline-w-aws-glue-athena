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
CustomerTrusted_node1742434565537 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-toke-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1742434565537")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1742435863956 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-toke-bucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1742435863956")

# Script generated for node SQL Query
SqlQuery3 = '''
select distinct c.customername, c.email, c.phone, c.birthday, c.serialnumber,
c.registrationdate, c.lastupdatedate, c.sharewithresearchasofdate,
c.sharewithpublicasofdate, c.shareWithFriendsAsOfDate
from customer_trusted c
inner join accelerometer_trusted a on a.email = c.email
'''
SQLQuery_node1742434692562 = sparkSqlQuery(glueContext, query = SqlQuery3, mapping = {"customer_trusted":CustomerTrusted_node1742434565537, "accelerometer_trusted":AccelerometerTrusted_node1742435863956}, transformation_ctx = "SQLQuery_node1742434692562")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1742434692562, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1742434412039", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1742435160353 = glueContext.getSink(path="s3://stedi-toke-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1742435160353")
AmazonS3_node1742435160353.setCatalogInfo(catalogDatabase="stedi_toke_db",catalogTableName="customer_curated")
AmazonS3_node1742435160353.setFormat("json")
AmazonS3_node1742435160353.writeFrame(SQLQuery_node1742434692562)
job.commit()