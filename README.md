# ENTRADA

ENTRADA - an open source big data tool for network data analytics.

Converting PCAP files containing DNS data to Apache Parquet files and sending the results to one of the following endpoints:  

ENTRADA converts PCAP files containing DNS data to Apache Parquet files and sends the results to one of the following endpoints:  
- Hadoop HDFS + Impala
- AWS S3 + Athena
- Local disk 

All workflow actions such moving data files around and creating database tables are handled by ENTRADA.  
For more information see the [ENTRADA wiki](https://github.com/SIDN/entrada/wiki).

## How to use

ENTRADA can be deployed using Docker, pull the docker image from Docker hub.  

```
   docker pull sidnlabs/entrada:<tag>

```

Download one of the example [Docker Compose scripts](https://github.com/SIDN/entrada/tree/master/docker-compose)  
Modify the environment variables for hostnames and filesystem paths to fit your requirements and then start the container.  


```
   docker-compose up -d

```


For more information about the configuration options see the [ENTRADA wiki](https://github.com/SIDN/entrada/wiki/Configuration).  

## License

This project is distributed under the LGPL, see [LICENSE](LICENSE).

## Attribution

When building a product or service using ENTRADA, we kindly request that you include the following attribution text in all advertising and documentation.
```
This product includes ENTRADA created by <a href="https://www.sidnlabs.nl">SIDN Labs</a>, available from
<a href="http://entrada.sidnlabs.nl">http://entrada.sidnlabs.nl</a>.
```
