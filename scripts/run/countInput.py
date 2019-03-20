#!/usr/bin/python3
import sys, boto3

def main(bucket, prefix, max):
    print(boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=int(max))["KeyCount"])

if __name__ == "__main__":
    main(*sys.argv[1:])
