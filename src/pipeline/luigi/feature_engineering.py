import luigi
import tempfile
import os
import pickle
import json
import pandas as pd
import luigi.contrib.s3
import io

from datetime import date
from src.pipeline.luigi.limpieza import Limpieza
from src.pipeline.luigi.limpieza_metadata import LimpiezaMetadata
from src.pipeline.feature_engineering import feature_engineering_all
from src.utils.constants import FEATURE_ENGINEERING_PATH, BUCKET
from src.pipeline.ingesta_almacenamiento import get_s3_resource

class FeatureEngineering(luigi.Task):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return Limpieza(ingesta=self.ingesta, date=self.date), LimpiezaMetadata(ingesta=self.ingesta, date=self.date)

    def run(self):
        data = None
        metadata = {
            "id": self.task_id,
            "step": 4
        }

        with self.input()[0][0].open("r") as input_target:
            data = input_target.read()

        inspect_df = pd.read_csv(io.StringIO(data))

        fe_data, fe_metadata = feature_engineering_all(inspect_df)
        fe_data_csv = fe_data.to_csv()

        metadata["metadata"] = fe_metadata

        with self.output()[0].open("w") as target:
            target.write(fe_data_csv)
        
        with self.output()[1].open("w") as metadata_target:
            metadata_target.write(json.dumps(metadata))

    def output(self):
        file_date = self.date or date.today().strftime("%Y-%m-%d")
        path = FEATURE_ENGINEERING_PATH
        output_path = "s3://{}/{}/{}-{}.csv".format(BUCKET, path, self.ingesta, file_date)
        s3_client = get_s3_resource()
        return luigi.contrib.s3.S3Target(
            path=output_path,
            client=s3_client
        ), luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "feature-engineering", f"feature-engineering-metadata-{self.ingesta}-{self.date}.json")
        )