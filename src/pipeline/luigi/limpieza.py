import luigi
import tempfile
import os
import pickle
import json
import pandas as pd
import luigi.contrib.s3

from datetime import date
from src.pipeline.luigi.almacenamiento import Almacenamiento
from src.pipeline.luigi.almacenamiento_metadata import AlmacenamientoMetadata
from src.pipeline.limpieza import clean_all
from src.utils.constants import LIMPIEZA_PATH, BUCKET
from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.utils.task import DPATask

class Limpieza(DPATask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return AlmacenamientoMetadata(ingesta=self.ingesta, date=self.date)

    def run(self):
        data = None
        metadata = {
            "id": self.task_id,
            "step": 3
        }

        with self.input()[0].open("r") as input_target:
            data = input_target.read()

        inspect_df = pd.DataFrame.from_dict(pickle.loads(data))

        clean_data, clean_metadata = clean_all(inspect_df)
        clean_csv_data = clean_data.to_csv()
        metadata["metadata"] = clean_metadata

        with self.output()[0].open("w") as target:
            target.write(clean_csv_data)
        
        with self.output()[1].open("w") as metadata_target:
            metadata_target.write(json.dumps(metadata))

    def output(self):
        file_date = self.date or date.today().strftime("%Y-%m-%d")
        path = LIMPIEZA_PATH
        output_path = "s3://{}/{}/{}-{}.csv".format(BUCKET, path, self.ingesta, file_date)
        s3_client = get_s3_resource()
        return luigi.contrib.s3.S3Target(
            path=output_path,
            client=s3_client
        ), luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "limpieza", f"limpieza-metadata-{self.ingesta}-{self.date}.json")
        )