import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1699339244015 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dhht-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1699339244015",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699339246263 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dhht-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1699339246263",
)

# Script generated for node Join
Join_node1699339248951 = Join.apply(
    frame1=CustomerTrusted_node1699339246263,
    frame2=AccelerometerLanding_node1699339244015,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1699339248951",
)

# Script generated for node Drop Fields
DropFields_node1699339402121 = DropFields.apply(
    frame=Join_node1699339248951,
    paths=[
        "serialnumber",
        "birthday",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1699339402121",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1699339253797 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1699339402121,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dhht-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1699339253797",
)

job.commit()
