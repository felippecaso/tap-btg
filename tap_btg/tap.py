"""BTG tap class."""

from __future__ import annotations

import json
import os
from typing import List

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_btg.streams import (
    BankingTransactionsStream,
    BTGStream,
    CreditTransactionsStream,
    InvestmentsTransactionsStream,
)


class TapBTG(Tap):
    """BTG tap class."""

    name = "tap-btg"

    config_jsonschema = th.PropertiesList(
        th.Property("file_password", th.StringType, secret=True),
        th.Property(
            "files",
            th.ArrayType(
                th.ObjectType(
                    th.Property("path", th.StringType, required=True),
                    th.Property(
                        "type",
                        th.StringType,
                        required=True,
                        allowed_values=[
                            "investments_transactions",
                            "banking_transactions",
                            "credit_transactions",
                        ],
                    ),
                )
            ),
            description="An array of file stream settings.",
        ),
        th.Property(
            "files_definition",
            th.StringType,
            description="A path to the JSON file holding an array of file settings.",
        ),
    ).to_dict()

    def get_file_configs(self) -> List[dict]:
        """Return a list of file configs.

        Either directly from the config.json or in an external file
        defined by files_definition.

        Returns:
            return (List): A list with file configs.
        """
        files = self.config.get("files")
        files_definition = self.config.get("files_definition")
        if files_definition:
            if os.path.isfile(files_definition):
                with open(files_definition, "r") as f:
                    files = json.load(f)
            else:
                self.logger.error(f"tap-btg: '{files_definition}' file not found")
                exit(1)
        if not files:
            self.logger.error("No file definitions found.")
            exit(1)
        return files

    def discover_streams(self) -> list[BTGStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        streams: list[BTGStream] = []
        file_configs = self.get_file_configs()
        for c in file_configs:
            if c.get("type") == "investments_transactions":
                streams.append(InvestmentsTransactionsStream(self, file_config=c))
            if c.get("type") == "credit_transactions":
                streams.append(CreditTransactionsStream(self, file_config=c))
            if c.get("type") == "banking_transactions":
                streams.append(BankingTransactionsStream(self, file_config=c))
        return streams


if __name__ == "__main__":
    TapBTG.cli()
