"""Stream type classes for tap-btg."""

from __future__ import annotations

import locale
import os
import re
from time import strptime
from typing import Iterable, List

import numpy as np
import pandas as pd
import pyexcel as pe
import tabula
from pypdf import PdfReader
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_btg.client import BTGStream

locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")


class InvestmentsTransactionsStream(BTGStream):
    name = "btg_investments_transactions"
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType, required=True),
        th.Property("description", th.StringType, required=True),
        th.Property("amount", th.NumberType, required=True),
    ).to_dict()

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Yield a generator of record-type dictionary objects.

        This function will yield a records based on the processed data.

        Args:
            context: An optional context as dictionary.

        Yields:
            yields: Records based on the processed data.
        """
        for file_path in self.get_file_paths():
            filename = file_path
            with open(filename, "rb") as o:
                reader = PdfReader(o)
                num_pages: int = len(reader.pages)

            tabula_reader = tabula.read_pdf(
                filename,
                pages=f"2-{num_pages}",
                multiple_tables=False,
                area=[17, 17, 794, 580],
                columns=[94, 340, 420, 505],
                pandas_options={"header": None},
                silent=True,
            )

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

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Yield a generator of record-type dictionary objects.

        This function will yield a records based on the processed data.

        Args:
            context: An optional context as dictionary.

        Yields:
            yields: Records based on the processed data.
        """
        file_password = self.file_config.get("file_password")
        if not file_password:
            self.logger.error("file_password is missing.")
        else:
            for file_path in self.get_file_paths():
                filename = file_path

                if filename.endswith(".pdf"):
                    file_year, file_month = [
                        int(
                            re.findall(r"(\d{4})-(\d{2})", os.path.basename(filename))[
                                0
                            ][i]
                        )
                        for i in (0, 1)
                    ]

                    btg_credit_raw_list: List[pd.DataFrame] = tabula.read_pdf(
                        filename,
                        pages="all",
                        password=file_password,
                        stream=True,
                        guess=False,
                        columns=[70, 225, 300, 340, 490, 562],
                        pandas_options={"header": None},
                        silent=True,
                    )  # type: ignore
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
                        if (file_month - strptime(df["date"][-3:], "%b").tm_mon) < 0:
                            return file_year - 1
                        else:
                            return file_year

                    btg_credit["transaction_year"] = btg_credit.apply(
                        _get_transaction_year, axis=1
                    ).astype(str)
                    btg_credit["date"] = pd.to_datetime(
                        btg_credit["date"] + " " + btg_credit["transaction_year"],
                        format="%d %b %Y",
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

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Yield a generator of record-type dictionary objects.

        This function will yield a records based on the processed data.

        Args:
            context: An optional context as dictionary.

        Yields:
            yields: Records based on the processed data.
        """
        for file_path in self.get_file_paths():
            filename = file_path

            if filename.endswith(".xls"):
                sheet = pe.get_records(
                    file_name=filename,
                    start_column=1,
                    start_row=10,
                    name_columns_by_row=0,
                )
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
