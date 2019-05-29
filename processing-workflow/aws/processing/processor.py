#    PcapProcessor, handles and transforms pcap files into parquet utilizing SIDN:s ENTRADA platform, built for AWS.
#
#    Copyright (C) 2019 Internetstiftelsen [https:/internetstiftelsen.se/en]
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

import asyncio
import json
import math
import subprocess
import time
import boto3
import re
import os
import errno
import datetime
import shutil
import pathlib
import sys

from pathlib import Path
from enum import Enum
from typing import Union


debug_mode = "False"


def date():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(*messages: str):
    print(f"[{date()}] :", *messages)


def debug(*messages: str):
    global debug_mode
    if debug_mode == "True":
        log("DEBUG", "::", *messages)


def str_path(*paths: Union[str, pathlib.Path], posix: bool = False):
    """Get the string of "path" formatted with regards to the os or if specified as posix.

    Args:
        paths: The paths to merge and format to match the os. Can be different formats, e.g. "start\\of", "the/path".

        posix: True if posix should be used regardless of os.

    Returns:
        str: Path in correct format with regards to the os or in posix format if posix is True.
    """
    if posix:
        return Path(*paths).as_posix()
    else:
        return str(Path(*paths))


def clean_key_parent(key: pathlib.Path):
    """Clear dashes from names like los-angeles to make sure files are correctly processed.

    Note that this function will prevent having a "server-location" format on the nameserver directories, either make
    sure no nameservers include this in their names or use "server_location" instead.

    Args:
        key:

    Returns:
        str: the nameserver (parent directory) of the key with dashes removed.
    """
    nameserver = key.parent.name
    return nameserver.replace("-", "")


def update(home_dir: pathlib.Path, tmp: str):
    """Update data used for enriching when processing pcap files.

    Uses entrada to find ip-addresses of OpenDNS and Google resolvers

    Args:
        home_dir: The home directory of entrada processing
        tmp: The location used as tmp storage for entrada
    """

    log("Finding OpenDNS and Google resolvers")
    subprocess.call([
        "java",
        "-cp",
        str_path(home_dir, "entrada/entrada-latest.jar"),
        "nl.sidn.pcap.Update",
        str_path(home_dir, "entrada/entrada-settings.properties"),
        tmp
    ])


def remove_zeroes(source_dir):
    """Remove leading zeroes from partition directories.

    The partition values can't include leading zeroes, e.g. "month=03", and need to be cleaned before uploading.

    Iterate through all years, months and days, and rename any directory that is found to have a leading zero.

    Args:
        source_dir (pathlib.Path): The directory which is parent to the generated file's partitions.
    """
    for year in source_dir.iterdir():
        if re.match(re.compile(r"year=\d{4}"), year.name) and year.is_dir():

            for month in year.iterdir():
                if re.match(re.compile(r"month=\d\d"), month.name) and month.is_dir():

                    # Replace leading zero
                    if re.match(re.compile(r'month=0\d'), month.name):
                        new_month = month.parents[0].joinpath('month=' + month.name[-1])
                        os.renames(str(month), new_month)
                        month = Path(new_month)

                    for day in month.iterdir():
                        if re.match(re.compile(r"day=0\d"), day.name) and day.is_dir():
                            new_day = day.parents[0].joinpath('day=' + day.name[-1])
                            os.renames(str(day), new_day)


def check_key(bucket: str, key: str):
    """Check if any object can be found by filtering the bucket with the given key.

    Args:
        bucket: Name of the S3 bucket to filter

        key: Object key to be used for filtering. Can be partial, e.g. "partial/key/example/", which allows for checking
        folders.

    Returns:
        bool: True if object is found, False if not

    """

    log(f"Checking if {key} exists")
    bucket_resource = boto3.resource("s3").Bucket(bucket)
    obj_collection = bucket_resource.objects.filter(Prefix=key, MaxKeys=1)
    try:
        # If nothing exists with the prefix, there will be nothing to iterate over
        next(obj_collection.__iter__())
    except StopIteration:
        return False
    else:
        return True


def report_exception(bucket: str, key: str, message: str, event: dict):
    client = boto3.client("s3")
    exception = {
        key: {
            "Exception": {
                "Time": str(datetime.datetime.now()),
                "Message": message
            },
            "Event": json.dumps(event)
        }
    }

    try:
        client.download_file(
            bucket,
            "lambda/exceptions.json",
            str_path("/tmp/exceptions.json")
        )
    except client.exceptions.ClientError:
        exception_dir = exception
    else:
        exception_dir = json.load(str_path("/tmp/exceptions.json"))

        if key in exception_dir.keys():
            if "Duplicates" not in exception_dir.keys():
                exception_dir["Duplicates"] = [key]
            else:
                exception_dir["Duplicates"].append(key)
        else:
            exception_dir.update(exception)

    json.dump(exception_dir, Path("/tmp/exceptions.json").open(mode="w"))

    client.upload_file(str_path("/tmp/exceptions.json"), bucket, str_path("lambda/exceptions.json"))


def archive(bucket: str, key: pathlib.Path, input_structure: str):
    """Create a copy of the "s3://bucket/key" at "s3://bucket/archive/part_of_key".

    Args:
        bucket: Name of bucket used, both contains the original and is the destination for the copy.

        key: Key of the object to archive.

        input_structure: Either "nameserver" or "date_nameserver". The parent directory structure of the object
    """
    class DirStructure(Enum):
        nameserver = 1  # e.g. "input/nameserver/filename.pcap"
        date_nameserver = 2  # e.g. "input/yyyy-mm-dd/nameserver/filename.pcap"

    client = boto3.client("s3")

    log(f"Archiving \"{key.name}\" to \"s3://{bucket}/archive\".")
    # Append the end of the input key to "archive/", starting at it's parent or grandparent,
    # depending on the structure used.
    archive_key = "archive/" + key.relative_to(
            key.parents[DirStructure[input_structure].value]
    ).as_posix()

    # Archive by copying the object from input to archive_key
    client.copy(
        {"Bucket": bucket, "Key": key.as_posix()},
        bucket,
        archive_key,
        ExtraArgs={"StorageClass": "GLACIER"}
    )
    # Delete the original object
    client.delete_object(Bucket=bucket, Key=key.as_posix())


async def archiving_handler(queue: asyncio.Queue, bucket, input_structure):
    """

    Args:
        queue:
        bucket:
        input_structure:

    Returns:

    """
    loop = asyncio.get_event_loop()
    while True:
        try:
            key = queue.get_nowait()
        except asyncio.QueueEmpty:
            break  # No item in queue means all keys are archived.
        else:
            await loop.run_in_executor(None, archive, bucket, Path(key), input_structure)


def download_pcap(bucket: str, key: pathlib.Path, processing: pathlib.Path):
    """Download pcap file from "s3://bucket/key" and store it in "/tmp/processing/nameserver"

    Args:
        bucket: The bucket which the file is located in

        key: Key of the pcap file to download. The parent directory of the file must be the nameserver

        processing: The processing directory, e.g. "/tmp/key_name/processing"

    Returns:

    """
    client = boto3.client("s3")
    nameserver = clean_key_parent(key)
    download_folder = processing / Path(nameserver)

    log(f"Downloading \"{key.name}\" to \"{download_folder}")
    client.download_file(
        bucket,
        key.as_posix(),
        str_path(download_folder, key.name)
    )


async def download_handler(queue: asyncio.Queue, bucket, processing, key_list):
    """As long as there are keys in queue, get key and start a downloader for it.

    Args:
        queue:
        bucket:
        processing:
        key_list:

    Returns:

    """
    loop = asyncio.get_event_loop()
    while True:
        try:
            key = queue.get_nowait()
        except asyncio.QueueEmpty:
            break  # No item in queue means download is completed.

        try:
            await loop.run_in_executor(None, download_pcap, bucket, Path(key), processing)
        except OSError as error:
            if error.errno == 28:  # No disk space
                # Since this key is no longer in the queue but hasn't been downloaded it needs to be removed from the
                # list so it doesn't get archived.
                log(f"Disk full, removing {key} from key_list")
                key_list.remove(key)
                raise


def upload(parent_dir, bucket: str, database: str, upload_name: str = ""):
    """Find any Parquet file below "parent_dir" and upload it to staging.

    Args:
        parent_dir: The directory in which entrada has stored the processed files.

        bucket: Name of the destination bucket.

        database: The name of the AWS Glue Data Catalog database that is used as metastore for the data.

        upload_name: Name of the pcap file that was processed (without the extension). This is used to correspondingly
        name the parquet file when uploaded to staging.
    """
    if upload_name == "":
        input_override = False
    else:
        input_override = True

    client = boto3.client("s3")
    file_found = False
    new_partition_found = False
    for root, dirs, files in os.walk(parent_dir):
        debug(root)
        debug(str(dirs))
        debug(str(files))
        debug("----")
        if re.search(re.compile(r"year=\d+/month=\d+/day=\d+/server=\w+"), str_path(root, posix=True)):
            for file in files:
                if re.match(re.compile(r'^.+\.parquet$'), file):

                    if not input_override:
                        upload_name = Path(file).name.replace(Path(file).suffix, "")

                    file_found = True

                    abs_path = Path(root).absolute() / Path(file)
                    log(f"Located \"{abs_path}\"")

                    # Cut the path to get partition directories and generated file
                    rel_path = abs_path.relative_to(abs_path.parents[4])
                    upload_key = Path("staging") / rel_path.parents[0] / Path(upload_name + ".parquet")

                    upload_folder = upload_key.parents[0]

                    if not new_partition_found:
                        new_partition_found = check_key(bucket, upload_folder.as_posix())
                        log("Found new partition, updating after all files are uploaded")

                    # If file is icmp data change upload location
                    if Path(*upload_key.parts[:2]) == Path("staging/icmpdata"):
                        upload_key = Path("icmpstaging/", *upload_key.partsl[2:])

                    log(f"Uploading \"{abs_path}\" to \"s3://{bucket}/{upload_key.as_posix()}\".")
                    client.upload_file(
                        str(abs_path),
                        bucket,
                        str(upload_key)
                    )
                    abs_path.unlink()  # Probably not necessary anymore but doesn't hurt.

    # Doing this for every file can lead to a TooManyRequestsError due to Athena's limits, it is also more costly.
    if new_partition_found:
        log("Updating staging metastore to get new partitions")
        update_metastore(database, bucket)

    if not file_found:
        log(f"Couldn't find any parquet files within \"{parent_dir}\"")


def update_metastore(database: str, bucket: str):
    """Use the Hive DDL statement "MSCK REPAIR" to update the metastore with new partitions.

    Args:
        database: The name of the AWS Glue Data Catalog database that is used as metastore for the data.

        bucket: Name of bucket to store query results

    Returns:

    """
    athena = boto3.client("athena")

    try:
        athena.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE {database}.staging",
            ResultConfiguration={
                "OutputLocation": f"s3://{bucket}/athena/processing_update/"
            }
        )

        athena.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE {database}.icmpstaging",
            ResultConfiguration={
                "OutputLocation": f"s3://{bucket}/athena/processing_update/"
            }
        )
    except athena.exceptions.InvalidRequestException as e:
        sys.stderr.write(
            f"Failed to update metastore with error: {e}\n" +
            "This is usually caused by an invalid database name, " +
            "please check your configuration is correct.\n"
        )
    except Exception as e:
        sys.stderr.write(f"Failed to update metastore with error: {e}\n")


def find_keys(bucket_name):
    """Find and return any pcap files inside "{bucket}/input".

    Args:
        bucket_name:

    Returns:
        list[str]: All keys of pcap files found in input.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    obj_collection = bucket.objects.filter(Prefix="input/")

    key_list = []
    for obj in obj_collection:
        if Path(obj.key).suffix == ".pcap":
            key_list.append(obj.key)

    return key_list


def process(nameserver: str, home_dir, processing, processed, tmp):
    """Use ENTRADA to process all pcap files for the nameserver given.

    When run in parallel logs get jumbled if they are continuously printed, therefore output is saved and printed in one
    go instead.

    Args:
        nameserver: The nameserver to be processed.

        home_dir:
        processing:
        processed:
        tmp:

    """
    start_message = f"[{date()}] : Starting processing of \"{nameserver}\"."
    output_log = subprocess.check_output(
        [
            "java",
            "-cp",
            str_path(home_dir, "entrada/entrada-latest.jar"),
            "nl.sidn.pcap.Main",
            nameserver,
            str_path(home_dir, "entrada/entrada-settings.properties"),
            str(processing), str(processed),
            str(tmp)
        ]
    )
    end_message = f"[{date()}] : Processing of \"{nameserver}\" complete."

    start_delimiter = "".join(["- " for _ in range(math.ceil(len(start_message)/2))])
    end_delimiter = "".join([*["- " for _ in range(math.ceil(len(end_message)/2))]])

    print(
        start_delimiter,  # results in "- - - - - ... - - - - - \n"
        start_message,
        start_delimiter,

        output_log.decode("utf-8"),

        end_delimiter,
        end_message,
        end_delimiter, "",
        sep="\n"
    )


async def processing_handler(queue, *args):
    """

    Args:
        queue:
        *args:

    Returns:

    """
    loop = asyncio.get_event_loop()
    while True:
        try:
            nameserver = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        else:
            await loop.run_in_executor(None, process, nameserver, *args)


async def main(
        processing_dir: pathlib.Path, home_dir: pathlib.Path, bucket: str,
        database: str, input_structure: str):
    """Locate and process any files in "s3://{bucket}/input".

    Args:
        processing_dir: The dir to download files to, this should be on a different disk than home_dir since files will

        download until the disk is full.

        home_dir: The base directory to work in.

        bucket: Name of the bucket to use. This bucket needs to contain all prerequisite files and will be used both as
        source and destination during processing.

        database: Name of the AWS Glue Data Catalog database that is used as metastore for the data.

        input_structure: The directory structure of the input key. Either "date_nameserver" (.../yyyy-mm-dd/server/file)
        or "nameserver" (.../server/file).
    """

    processing = processing_dir
    processed = Path(home_dir, 'processed')
    tmp = Path(home_dir, "tmp")

    key_list = find_keys(bucket)
    nameserver_list = []

    if len(key_list) == 0:
        log("No new files in input.")
        return False
    else:
        log(f"Found {len(key_list)} files in input")

        # Clear old directories.
        if processed.exists():
            for obj in processed.iterdir():
                if obj.is_dir():
                    shutil.rmtree(obj)

        update(home_dir, str(tmp))

        # -- --
        # -- Download preparations --
        # -- --
        download_queue = asyncio.Queue()

        for key in key_list:
            nameserver = clean_key_parent(Path(key))
            if nameserver not in nameserver_list:
                nameserver_list.append(nameserver)

                # Create required directory if it doesn't exist
                if not Path(processing, nameserver).exists():
                    os.makedirs(Path(processing, nameserver))
                if not Path(processed, nameserver).exists():
                    os.makedirs(Path(processed, nameserver))

            # Add the key to the queue
            await download_queue.put(key)
        # -- --

        # -- --
        # -- Download files in parallel --
        # -- --
        downloader_count = 10
        download_results = await asyncio.gather(
            *[
                download_handler(
                    download_queue, bucket, processing, key_list
                ) for _ in range(downloader_count)
                # Create downloader_handler tasks to download data in parallel (the handlers are asynchronous but the
                # downloads are parallel).
            ],
            return_exceptions=True
            # return_exceptions is used since using try except would lead to the code moving on once the first exception
            # occurs, while this is not fatal it could potentially cause problems (and it looks bad in the logs...)
        )
        # Check if any task raised a disk full exception.
        disk_full_error = os.error(errno.ENOSPC, os.strerror(errno.ENOSPC))
        for item in download_results:
            if type(item) == type(disk_full_error) \
                    and [item.errno, item.strerror] == [disk_full_error.errno, disk_full_error.strerror]:
                log("Download disk is full, removing keys which have not been downloaded from list")
                # Keep only the keys which have been downloaded (removed from queue)
                # When this error occurs any keys which have been taken from queue but failed to be downloaded will be
                # removed from key_list by the download_handlers while the rest are removed below.
                while True:  # Go through all keys left in the queue and break when it's empty
                    try:
                        key_to_remove = download_queue.get_nowait()
                        log(f"Removing {key_to_remove} from key_list")
                        key_list.remove(key_to_remove)
                    except asyncio.QueueEmpty:
                        break
                break
        # -- --

        # -- --
        # -- Process data for all nameservers in parallel --
        # -- --
        processing_queue = asyncio.Queue()
        for nameserver in nameserver_list:
            await processing_queue.put(nameserver)

        processor_count = 3
        await asyncio.gather(
            *[
                processing_handler(
                    processing_queue, home_dir, processing, processed, tmp
                ) for _ in range(processor_count)
            ]  # Same principle as when downloading.
        )
        # -- --

        # -- --
        # -- Post processing --
        # -- --
        # Clear leading zeroes from month and day dirs
        for nameserver in nameserver_list:
            for subdir in Path(processed, nameserver).iterdir():
                remove_zeroes(subdir)

        upload(processed, bucket, database)

        # Archive the keys now that processing is completed.
        archive_queue = asyncio.Queue()
        for key in key_list:
            await archive_queue.put(key)

        log(f"Archiving {archive_queue.qsize()} keys")
        archiver_count = 10
        await asyncio.gather(
            *[
                archiving_handler(
                    archive_queue, bucket, input_structure
                ) for _ in range(archiver_count)
            ]  # Same principle as when downloading and processing.
        )
        # -- --
        return True


if __name__ == '__main__':
    home = sys.argv[1]
    config_file = Path(home, "config.json")
    with config_file.open() as f:
        config = json.load(f)

    if not Path(home, "tmp/").exists():
        os.makedirs(Path(home, "tmp/"))

    while True:
        # Update geo ip database to ensure it exists.
        subprocess.call(
            [
                "sh",
                str_path(home, "entrada/run_update_geo_ip_db.sh"),
                str_path(home, "tmp/")
            ]
        )
        last_update = datetime.datetime.today().date()

        while True:
            if not asyncio.run(main(**config)):
                time.sleep(60)
                # If main returned true (found and processed files) then don't sleep since there might be more files.

            today = datetime.datetime.today().date()
            is_thursday = today.weekday() == 3
            is_first_of_month = today.day < 8
            if is_thursday and is_first_of_month and last_update != today:
                break
                # The database gets updated the first tuesday each month, download it the first thursday of every month
                # to guarantee it has been uploaded. Also make sure it hasn't already been downloaded today.
