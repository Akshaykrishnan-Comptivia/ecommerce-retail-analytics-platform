import os
import shutil
import zipfile

import pandas as pd
import requests
import yaml

from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession


class PublicDataDownloader:

    AMAZON_REVIEWS_BASE_URL = (
        "https://huggingface.co/datasets/McAuley-Lab/"
        "Amazon-Reviews-2023/resolve/main/raw/review_categories/"
        "{category}.jsonl"
    )

    def __init__(self, spark: SparkSession, config_path: str):

        self.spark = spark

        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.landing_zone = (
            self.config["storage"]["raw_landing_zone"]
        )

    def download_all(self):

        print("=" * 50)
        print("Starting Public Data Download")
        print("=" * 50)

        for source_name, source_config in self.config["data_sources"].items():

            try:
                print(f"\nDownloading: {source_name}")

                self._download_source(
                    source_name,
                    source_config
                )

                print(f"Completed: {source_name}")

            except Exception as e:
                print(f"Failed: {source_name}")
                print(str(e))

        print("\nDownload Complete")

    def _download_source(
        self,
        source_name: str,
        source_config: dict
    ):

        fmt = source_config["format"]

        domain = source_config["domain"]

        output_dir = (
            f"{self.landing_zone}/"
            f"{domain}/"
            f"{source_name}"
        )

        if fmt == "kaggle":

            self._download_kaggle(
                source_config,
                output_dir
            )

        elif fmt == "zip":

            self._download_zip(
                source_config,
                output_dir
            )

        elif fmt == "jsonl":

            self._download_jsonl(
                source_config,
                output_dir
            )

        else:
            raise ValueError(
                f"Unsupported format: {fmt}"
            )

    def _copy_file(
        self,
        source_path: str,
        destination_path: str
    ):

        os.makedirs(
            os.path.dirname(destination_path),
            exist_ok=True
        )

        shutil.copy(
            source_path,
            destination_path
        )

    def _download_kaggle(
        self,
        source_config: dict,
        output_dir: str
    ):

        dataset = source_config["kaggle_dataset"]

        local_dir = "/tmp/kaggle_download"

        os.makedirs(local_dir, exist_ok=True)

        api = KaggleApi()

        api.authenticate()

        api.dataset_download_files(
            dataset,
            path=local_dir,
            unzip=True
        )

        csv_files = [
            file
            for file in os.listdir(local_dir)
            if file.endswith(".csv")
        ]

        for file in csv_files:

            local_path = os.path.join(
                local_dir,
                file
            )

            destination_path = (
                f"{output_dir}/{file}"
            )

            self._copy_file(
                local_path,
                destination_path
            )

            print(f"Copied: {file}")

    def _download_zip(
        self,
        source_config: dict,
        output_dir: str
    ):

        url = source_config["url"]

        inner_file = source_config["inner_file"]

        local_zip = "/tmp/retail.zip"

        extract_dir = "/tmp/retail_extract"

        os.makedirs(extract_dir, exist_ok=True)

        response = requests.get(url)

        with open(local_zip, "wb") as f:
            f.write(response.content)

        with zipfile.ZipFile(local_zip, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        xlsx_path = os.path.join(
            extract_dir,
            inner_file
        )

        workbook = pd.ExcelFile(xlsx_path)

        for sheet_name in workbook.sheet_names:

            df = pd.read_excel(
                xlsx_path,
                sheet_name=sheet_name
            )

            safe_name = (
                sheet_name
                .replace(" ", "_")
                .replace("-", "_")
            )

            local_csv = f"/tmp/{safe_name}.csv"

            df.to_csv(
                local_csv,
                index=False
            )

            destination_path = (
                f"{output_dir}/{safe_name}.csv"
            )

            self._copy_file(
                local_csv,
                destination_path
            )

            print(f"Copied: {safe_name}")

    def _download_jsonl(
        self,
        source_config: dict,
        output_dir: str
    ):

        category = source_config["category"]

        url = source_config.get(
            "url",
            self.AMAZON_REVIEWS_BASE_URL.format(
                category=category
            )
        )

        max_records = source_config["max_records"]

        local_path = "/tmp/reviews.jsonl"

        response = requests.get(
            url,
            stream=True
        )

        count = 0

        with open(
            local_path,
            "w",
            encoding="utf-8"
        ) as f:

            for line in response.iter_lines():

                if line:

                    f.write(
                        line.decode("utf-8") + "\n"
                    )

                    count += 1

                    if count >= max_records:
                        break

        destination_path = (
            f"{output_dir}/{category}.jsonl"
        )

        self._copy_file(
            local_path,
            destination_path
        )

        print(
            f"Copied {count} records "
            f"for {category}"
        )