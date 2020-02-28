# ENTRADA

Entrada - A tool for DNS big data analytics  

ENTRADA processes DNS data (PCAP-files) from an input location (local, HDFS or S3) and converts and enriches the data to Apache Parquet format, finally sending the results to one of following endpoints: 
- HDFS + Impala (hadoop)
- S3 + Athena (aws)
- Local disk (local)

See the [database schema](https://entrada.sidnlabs.nl/datamodel/) for more information about all the database columns.  

The data is enriched by adding the following details to each row.   
- Geolocation (Country)
- Autonomous system (ASN) details
- Detection of public resolvers (Google, OpenDNS, Quad9 and Cloudflare)
- TCP round-trip time (RTT) 

Apache Impala, AWS Athena or Apache Spark can be used to analyse the generated Parquet data.  

ENTRADA handles the required workflow actions such as:  
- Loading and archiving PCAP files
- Converting and enriching data
- Creating database schema and tables
- Creating a S3 bucket
- Configuring S3 security policy and encryption
- Creating filesystem directories
- Moving data files around
- Uploading data to HDFS or S3
- Compacting Parquet files on HDFS or S3

For more information see the [ENTRADA website](https://entrada.sidnlabs.nl/).

## How to use

ENTRADA is deployed using [Docker Compose](https://docs.docker.com/compose/), download one of the example [Docker Compose scripts](https://github.com/SIDN/entrada/tree/master/docker-compose) and save it as `docker-compose.yml` and then edit the script to configure the environment variables to fit your requirements.  
Start the container using the `docker-compose` command:  


```
   docker-compose up

```


For more more detailed deployment instructions and available onfiguration options see the [ENTRADA website](https://entrada.sidnlabs.nl/about/installation/).  

## License

This project is distributed under the GPLv3, see [LICENSE](LICENSE).

## Attribution

When building a product or service using ENTRADA, we kindly request that you include the following attribution text in all advertising and documentation.
```
This product includes ENTRADA created by <a href="https://www.sidnlabs.nl">SIDN Labs</a>, available from
<a href="http://entrada.sidnlabs.nl">http://entrada.sidnlabs.nl</a>.
```
