"""Custom client handling, including BTGStream base class."""

from __future__ import annotations

import os
from typing import List

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

    def get_file_paths(self) -> list:
        """Return a list of file paths to read.

        This tap accepts file names and directories so it will detect
        directories and iterate files inside.

        Returns:
            return (List): A list with file paths to read.

        Raises:
            Exception: When file path does not exist.
        """
        # Cache file paths so we dont have to iterate multiple times
        if self.file_paths:
            return self.file_paths

        file_path = self.file_config["path"]
        if not os.path.exists(file_path):
            raise Exception(f"File path does not exist {file_path}")

        file_paths = []
        if os.path.isdir(file_path):
            clean_file_path = os.path.normpath(file_path) + os.sep
            for filename in os.listdir(clean_file_path):
                file_path = clean_file_path + filename
                file_paths.append(file_path)
        else:
            file_paths.append(file_path)

        if not file_paths:
            raise Exception(
                f"Stream '{self.name}' has no acceptable files. \
                    See warning for more detail."
            )
        self.file_paths = file_paths
        return file_paths
