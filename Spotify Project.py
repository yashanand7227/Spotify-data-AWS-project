import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node artist
artist_node1741900010238 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://fresh981349871294837194870203847019394/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1741900010238")

# Script generated for node album
album_node1741900013046 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://fresh981349871294837194870203847019394/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1741900013046")

# Script generated for node tracks
tracks_node1741900015025 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://fresh981349871294837194870203847019394/staging/track.csv"], "recurse": True}, transformation_ctx="tracks_node1741900015025")

# Script generated for node Join Album & Artist
JoinAlbumArtist_node1741900287842 = Join.apply(frame1=album_node1741900013046, frame2=artist_node1741900010238, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoinAlbumArtist_node1741900287842")

# Script generated for node Join
Join_node1741901119924 = Join.apply(frame1=JoinAlbumArtist_node1741900287842, frame2=tracks_node1741900015025, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Join_node1741901119924")

# Script generated for node Drop Fields
DropFields_node1741901266509 = DropFields.apply(frame=Join_node1741901119924, paths=["id", "`.track_id`"], transformation_ctx="DropFields_node1741901266509")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1741901266509, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741901013725", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1741901469383 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1741901266509, connection_type="s3", format="glueparquet", connection_options={"path": "s3://fresh981349871294837194870203847019394/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1741901469383")

job.commit()