# entrada-aws
Modified version of ENTRADA with a rewritten workflow made to be used in AWS.

Although this branch SHOULD be stable enough for a production environment it has not been tested enough to guarantee this, please submit any issues you encounter.

## Changes
While fitting Entrada into AWS, the system was altered in many ways. The main change is mostly moving away from directly using clusters. 

Processing is done in an AWS EC2 instance which downloads pcap files and uploads parquet files from/to s3. This entire process is handled by a python program which checks input for new files every minute.

The one part still done by a Hadoop cluster (that is handled by ENTRADA) is the move from Staging to DWH, because AWS Elastic MapReduce offers a convenient way to merge and move parquet files.

Querying data is done through AWS Athena, a serverless, scalable query service which uses Presto on Hadoop clusters in the background. This allows for easy and cheap queries compared to starting clusters on demand or having one running continuously. The pay model is based only on the amount of data scanned in your query.

## Installation
Although a large part of the resources used can be created with the cloudformation template provided, some prerequisites need to be manually created.

1. Create a bucket which will be used for storage of data and programs (an existing bucket can be used as well).
2. Create the following directories in the top level of your bucket, "archive". "athena", "entrada", "icmpqueries", "icmpstaging", "input", "queries" and "staging".
3. Download and upload the contents of the release to "s3://bucket/entrada/"
4. Create a Glue Data Catalog database and create tables in Athena using the Sql statements provided in the release, replace DATABASE with the name of the database and BUCKET with the name of your bucket.
5. Set up lifecycle rules to delete archived pcap files, they are archived using glacier which has a storage time of 90 days no matter when removed that is the minimum amount to pay (note that they will not be visible in glacier due to them being created with s3).

Now that the prerequisites are handled, create a cloudformation stack using the template which should be located in the entrada folder of your bucket. This will create an ec2 instance that will handle processing, a lambda function which when triggered by an event will start an EMR cluster to handle moving data from staging to the data warehouse/queries. 

To query data use AWS Athena (you could also use an EMR cluster launched with glue data catalog as its metastore, however Athena is recommended for most if not all use cases). 
