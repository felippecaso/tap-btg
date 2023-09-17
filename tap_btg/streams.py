"""Stream type classes for tap-btg."""

from __future__ import annotations

import os
import re
import tempfile
import dateparser
from typing import Iterable, List

import boto3
import numpy as np
import pandas as pd
import pyexcel as pe
import tabula
from pypdf import PdfReader
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_btg.client import BTGStream


class InvestmentsTransactionsStream(BTGStream):
    name = "btg_investments_transactions"
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType, required=True),
        th.Property("description", th.StringType, required=True),
        th.Property("amount", th.NumberType, required=True),
    ).to_dict()

    def read_pdf_from_s3(self, s3_bucket: str, s3_key: str):
        # Use boto3 to download the file from S3 and then read it
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        pdf_content = response["Body"].read()

        # Create a temporary file-like object for tabula to read from
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(pdf_content)
            temp_file_path = temp_file.name

        try:
            num_pages = self.get_pdf_num_pages(temp_file_path)
            tabula_reader = tabula.read_pdf(
                temp_file_path,
                pages=f"2-{num_pages}",
                multiple_tables=False,
                area=[17, 17, 794, 580],
                columns=[94, 340, 420, 505],
                pandas_options={"header": None},
                silent=True,
            )
        finally:
            # Clean up the temporary file
            os.remove(temp_file_path)
        return tabula_reader

    def read_pdf_from_local(self, file_path: str):
        num_pages = self.get_pdf_num_pages(file_path)
        tabula_reader = tabula.read_pdf(
            file_path,
            pages=f"2-{num_pages}",
            multiple_tables=False,
            area=[17, 17, 794, 580],
            columns=[94, 340, 420, 505],
            pandas_options={"header": None},
            silent=True,
        )
        return tabula_reader

    def get_pdf_num_pages(self, file_path: str) -> int:
        with open(file_path, "rb") as o:
            reader = PdfReader(o)
            num_pages: int = len(reader.pages)
        return num_pages

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Yield a generator of record-type dictionary objects.

        This function will yield a records based on the processed data.

        Args:
            context: An optional context as dictionary.

        Yields:
            yields: Records based on the processed data.
        """
        for file_path in self.get_file_paths():
            if file_path.startswith("s3://"):
                s3_bucket, s3_key = file_path.split("/", 3)[2:]
                tabula_reader = self.read_pdf_from_s3(s3_bucket, s3_key)
            else:
                tabula_reader = self.read_pdf_from_local(file_path)

            btg_investments_raw: pd.DataFrame = tabula_reader[0]  # type: ignore

            btg_investments_raw = btg_investments_raw.rename(
                columns=btg_investments_raw.iloc[1]
            ).drop(btg_investments_raw.index[:2])
            btg_investments_raw["Sign"] = np.where(
                btg_investments_raw["Débito"].isnull(), 1, -1
            )

            btg_investments_raw["Valor"] = (
                btg_investments_raw.loc[:, ["Débito", "Crédito"]]
                .bfill(axis=1)
                .iloc[:, 0]
                .astype(str)
            )
            btg_investments_raw["Valor"] = [
                x.replace(".", "").replace(",", ".")
                for x in btg_investments_raw["Valor"]
            ]
            btg_investments_raw["Valor"] = btg_investments_raw[
                "Sign"
            ] * btg_investments_raw["Valor"].astype(float)

            btg_investments: pd.DataFrame = btg_investments_raw.rename(
                columns={"Descrição": "description", "Data": "date", "Valor": "amount"}
            ).drop(columns=["Débito", "Crédito", "Sign", "Saldo"])
            btg_investments.drop(btg_investments.tail(3).index, inplace=True)
            btg_investments.drop(btg_investments.head(1).index, inplace=True)
            btg_investments["date"] = pd.to_datetime(
                btg_investments["date"], dayfirst=True
            )
            btg_investments = btg_investments.convert_dtypes()

            for r in btg_investments.to_dict("records"):
                yield r


class CreditTransactionsStream(BTGStream):
    name = "btg_credit_transactions"
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType, required=True),
        th.Property("description", th.StringType, required=True),
        th.Property("amount", th.NumberType, required=True),
        th.Property("bill_month", th.DateTimeType, required=True),
    ).to_dict()

    def read_pdf_from_s3(self, s3_bucket: str, s3_key: str, file_password: str):
        # Use boto3 to download the file from S3 and then read it
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        pdf_content = response["Body"].read()

        # Create a temporary file-like object for tabula to read from
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(pdf_content)
            temp_file_path = temp_file.name

        try:
            btg_credit_raw_list: List[pd.DataFrame] = tabula.read_pdf(
                temp_file_path,
                pages="all",
                password=file_password,
                stream=True,
                guess=False,
                columns=[70, 225, 300, 340, 490, 562],
                pandas_options={"header": None},
                silent=True,
            )  # type: ignore
        finally:
            # Clean up the temporary file
            os.remove(temp_file_path)
        return btg_credit_raw_list

    def read_pdf_from_local(self, file_path: str, file_password: str):
        btg_credit_raw_list: List[pd.DataFrame] = tabula.read_pdf(
            file_path,
            pages="all",
            password=file_password,
            stream=True,
            guess=False,
            columns=[70, 215, 300, 340, 490, 562],
            pandas_options={"header": None},
            silent=True,
        )  # type: ignore
        return btg_credit_raw_list

    @staticmethod
    def extract_file_name(file_path: str) -> str:
        if file_path.startswith("s3://"):
            # Extract the S3 key from the URL and remove the file extension
            s3_key = file_path.split("/")[-1]
            file_name_without_extension = s3_key.rsplit(".", 1)[0]
        else:
            # Extract the file name and remove the file extension
            file_name = os.path.basename(file_path)
            file_name_without_extension = os.path.splitext(file_name)[0]
        return file_name_without_extension

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Yield a generator of record-type dictionary objects.

        This function will yield a records based on the processed data.

        Args:
            context: An optional context as dictionary.

        Yields:
            yields: Records based on the processed data.
        """
        file_password = self.config.get("file_password")
        if not file_password:
            self.logger.error("file_password is missing.")
        else:
            for file_path in self.get_file_paths():
                if file_path.endswith(".pdf"):
                    if file_path.startswith("s3://"):
                        s3_bucket, s3_key = file_path.split("/", 3)[2:]
                        btg_credit_raw_list = self.read_pdf_from_s3(
                            s3_bucket, s3_key, file_password
                        )
                    else:
                        btg_credit_raw_list = self.read_pdf_from_local(
                            file_path, file_password
                        )

                    file_year, file_month = [
                        int(
                            re.findall(
                                r"(\d{4})-(\d{2})", self.extract_file_name(file_path)
                            )[0][i]
                        )
                        for i in (0, 1)
                    ]

                    btg_credit_raw: pd.DataFrame = pd.concat(btg_credit_raw_list[1:])

                    condition_1 = btg_credit_raw.iloc[:, 0].str.contains(
                        r"^[0-9]{2}\s[a-zA-Z]{3}", regex=True, na=False
                    )
                    condition_2 = btg_credit_raw.iloc[:, 3].str.contains(
                        r"^[0-9]{2}\s[a-zA-Z]{3}", regex=True, na=False
                    )
                    condition_3 = btg_credit_raw.iloc[:, 1].str.contains(
                        "Cotação|Conversão", regex=True, na=False
                    )
                    condition_4 = btg_credit_raw.iloc[:, 4].str.contains(
                        "Cotação|Conversão", regex=True, na=False
                    )

                    btg_credit_raw = btg_credit_raw[
                        (condition_1 | condition_2 | condition_3 | condition_4)
                    ]
                    btg_credit: pd.DataFrame = pd.DataFrame(
                        np.concatenate(
                            [btg_credit_raw[[0, 1, 2]], btg_credit_raw[[3, 4, 5]]],
                            axis=0,
                        ),
                        columns=["date", "description", "amount"],
                    )
                    btg_credit.dropna(how="all", inplace=True)

                    usd_transactions_mask = btg_credit["amount"].str.contains(
                        "US$", regex=False
                    )
                    for i in btg_credit.loc[usd_transactions_mask].index:
                        btg_credit.loc[i, "amount"] = btg_credit.loc[i + 2, "amount"]
                        btg_credit.drop([i + 1, i + 2], inplace=True)

                    btg_credit.reset_index(drop=True, inplace=True)

                    btg_credit["amount"] = np.where(
                        btg_credit["amount"].str.startswith("-"),
                        btg_credit["amount"].str.lstrip("- R$"),
                        "-" + btg_credit["amount"].str.lstrip("R$ "),
                    )
                    btg_credit["amount"] = btg_credit["amount"].str.replace(
                        ".", "", regex=False
                    )
                    btg_credit["amount"] = btg_credit["amount"].str.replace(
                        ",", ".", regex=False
                    )
                    btg_credit["amount"] = pd.to_numeric(btg_credit["amount"])

                    def _get_transaction_year(df):
                        if (file_month - dateparser.parse(
                            df["date"][-3:],
                            date_formats=["%b"],
                            languages=["pt"]
                        ).month) < 0:
                            return file_year - 1
                        else:
                            return file_year

                    btg_credit["transaction_year"] = btg_credit.apply(
                        _get_transaction_year, axis=1
                    ).astype(str)

                    def _create_transaction_date(df):
                        return dateparser.parse(
                            df["date"] + " " + df["transaction_year"],
                            date_formats=["%d %^b %Y"],
                            languages=["pt"]
                        )

                    btg_credit["date"] = btg_credit.apply(
                        _create_transaction_date, axis=1
                    )

                    btg_credit.drop("transaction_year", axis=1, inplace=True)

                    btg_credit["installment_number"] = (
                        btg_credit["description"]
                        .str.extract(r"\(([0-9])\/[0-9]\)")
                        .fillna("0")
                        .astype(int)
                    )
                    btg_credit["installment_total"] = (
                        btg_credit["description"]
                        .str.extract(r"\([0-9]\/([0-9])\)")
                        .fillna("0")
                        .astype(int)
                    )
                    btg_credit["date"] = btg_credit.apply(
                        lambda row: row["date"]
                        + pd.DateOffset(months=row["installment_number"] - 1)
                        if row["installment_total"] != 0
                        else row["date"],
                        axis=1,
                    )
                    btg_credit.drop("installment_number", axis=1, inplace=True)
                    btg_credit.drop("installment_total", axis=1, inplace=True)

                    btg_credit.drop(btg_credit.head(1).index, inplace=True)

                    btg_credit["bill_month"] = pd.to_datetime(
                        f"{file_year}-{file_month}-01"
                    )

                    btg_credit = btg_credit.convert_dtypes()

                    for r in btg_credit.to_dict("records"):
                        yield r


class BankingTransactionsStream(BTGStream):
    name = "btg_banking_transactions"
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType, required=True),
        th.Property("description", th.StringType, required=True),
        th.Property("amount", th.NumberType, required=True),
    ).to_dict()

    def read_xls_from_s3(self, s3_bucket: str, s3_key: str):
        # Use boto3 to download the file from S3 and then read it
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        xls_content = response["Body"].read()

        # Create a temporary file-like object for tabula to read from
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xls") as temp_file:
            temp_file.write(xls_content)
            temp_file_path = temp_file.name

        try:
            sheet = pe.get_records(
                file_name=temp_file_path,
                start_column=1,
                start_row=10,
                name_columns_by_row=0,
            )
        finally:
            # Clean up the temporary file
            os.remove(temp_file_path)
        return sheet

    def read_xls_from_local(self, file_path: str):
        sheet = pe.get_records(
            file_name=file_path,
            start_column=1,
            start_row=10,
            name_columns_by_row=0,
        )
        return sheet

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Yield a generator of record-type dictionary objects.

        This function will yield a records based on the processed data.

        Args:
            context: An optional context as dictionary.

        Yields:
            yields: Records based on the processed data.
        """
        for file_path in self.get_file_paths():
            if file_path.endswith(".xls"):
                if file_path.startswith("s3://"):
                    s3_bucket, s3_key = file_path.split("/", 3)[2:]
                    sheet = self.read_xls_from_s3(s3_bucket, s3_key)
                else:
                    sheet = self.read_xls_from_local(file_path)
                btg_banking: pd.DataFrame = pd.DataFrame(sheet)

                btg_banking = btg_banking[
                    ["Data e hora", "Categoria", "Transação", "Descrição", "Valor"]
                ]
                btg_banking = btg_banking.replace("", np.nan)
                btg_banking = btg_banking.dropna(subset="Valor")
                btg_banking.drop(
                    btg_banking[btg_banking["Data e hora"] == "Data e hora"].index,
                    inplace=True,
                )
                btg_banking["Data e hora"] = pd.to_datetime(
                    btg_banking["Data e hora"], dayfirst=True
                )
                btg_banking["description"] = (
                    btg_banking["Categoria"]
                    + " - "
                    + btg_banking["Transação"]
                    + " - "
                    + btg_banking["Descrição"]
                )
                btg_banking = btg_banking.rename(
                    columns={"Data e hora": "date", "Valor": "amount"}
                ).drop(columns=["Categoria", "Transação", "Descrição"])
                btg_banking = btg_banking.convert_dtypes()

                for r in btg_banking.to_dict("records"):
                    yield r
