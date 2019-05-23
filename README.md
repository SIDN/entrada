# entrada-aws
Modified version of ENTRADA with a rewritten workflow made to be used in AWS.

# Changes
While fitting Entrada into AWS the system was altered in many ways. The main point is largely moving away from directly using clusters. 

Processing is done in an AWS EC2 instance which downloads pcap files and uploads parquet files from/to s3. This entire process is handled by a python program which checks input for new files every minute.

The one part still done by a Hadoop cluster (that is handled by ENTRADA) is the move from Staging to DWH, because AWS Elastic MapReduce offers a convenient way to merge and move parquet files.

Querying data is done through AWS Athena, a serverless, scalable query service which in the background uses Presto on Hadoop clusters. This allows for easy and cheap queries compared to starting clusters on demand or having one running continuously.
