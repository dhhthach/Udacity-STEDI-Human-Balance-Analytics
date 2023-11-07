import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1699335966264 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dhht-lakehouse/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1699335966264",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource
where shareWithResearchAsOfDate is not null
"""
SQLQuery_node1699337425228 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": CustomerLanding_node1699335966264},
    transformation_ctx="SQLQuery_node1699337425228",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699338174661 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1699337425228,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dhht-lakehouse/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1699338174661",
)

job.commit()
