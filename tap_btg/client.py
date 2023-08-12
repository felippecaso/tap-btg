"""Custom client handling, including BTGStream base class."""

from __future__ import annotations

import os
from typing import List

import boto3
from singer_sdk.streams import Stream


class BTGStream(Stream):
    """Stream class for BTG streams."""

    file_paths: List[str] = []

    def __init__(self, *args, **kwargs):
        """Init BTGStream.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        # cache file_config so we dont need to go iterating the config list again later
        self.file_config = kwargs.pop("file_config")
        super().__init__(*args, **kwargs)

    def list_s3_files_in_folder(self, s3_bucket: str, s3_folder: str) -> List[str]:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_folder)
        file_paths = []
        for item in response.get("Contents", []):
            if not item["Key"].endswith("/"):
                file_paths.append(f"s3://{s3_bucket}/{item['Key']}")
        return file_paths

    def get_file_paths(self) -> list:
        """Return a list of file paths to read.

        This tap accepts file names and directories so it will detect
        directories and iterate files inside, both in local files and
        in AWS S3.

        Returns:
            return (List): A list with file paths to read.

        Raises:
            Exception: When file path does not exist.
        """
        # Cache file paths so we dont have to iterate multiple times
        if self.file_paths:
            return self.file_paths

        file_path = self.file_config["path"]
        if not file_path:
            raise Exception("file_path is not provided.")

        if file_path.startswith("s3://"):
            path_split = file_path.split("/")
            s3_bucket, s3_key = path_split[2], "/".join(path_split[3:])
            if s3_key.endswith("/"):
                # If the S3 URL points to a folder, list all files inside
                file_paths = self.list_s3_files_in_folder(s3_bucket, s3_key)
                self.file_paths = file_paths
            else:
                # Treat the S3 URL as a single file
                self.file_paths = [file_path]
                file_paths = [file_path]
        elif os.path.isdir(file_path):
            clean_file_path = os.path.normpath(file_path) + os.sep
            file_paths = []
            for filename in os.listdir(clean_file_path):
                file_paths.append(clean_file_path + filename)
            self.file_paths = file_paths
        elif os.path.exists(file_path):
            self.file_paths = [file_path]
            file_paths = [file_path]
        else:
            raise Exception(f"File path does not exist: {file_path}")

        if not self.file_paths:
            raise Exception(f"No acceptable files found for stream '{self.name}'.")

        return file_paths
