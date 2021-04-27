import luigi
import tempfile
import os
import pickle
import json
import pandas as pd
import luigi.contrib.s3
import io

from datetime import date
from src.pipeline.luigi.feature_engineering_metadata import FeatureEngineeringMetadata
from src.pipeline.entrenamiento import run_magic_loop
from src.utils.constants import ENTRENAMIENTO_PATH, BUCKET
from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.utils.task import DPATask

class Entrenamiento(DPATask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return FeatureEngineeringMetadata(ingesta=self.ingesta, date=self.date)

    def run(self):
        data = None
        metadata = {
            "id": self.task_id,
            "step": 5
        }

        with self.input()[0].open("r") as input_target:
            data = input_target.read()

        inspect_df = pd.read_csv(io.StringIO(data))

        training_data, training_metadata = run_magic_loop(inspect_df)

        metadata["metadata"] = training_metadata

        with self.output()[0].open("w") as target:
            target.write(pickle.dumps(training_data))
        
        with self.output()[1].open("w") as metadata_target:
            metadata_target.write(json.dumps(metadata))


    def output(self):
        file_date = self.date or date.today().strftime("%Y-%m-%d")
        path = ENTRENAMIENTO_PATH
        output_path = "s3://{}/{}/{}-{}.pkl".format(BUCKET, path, self.ingesta, file_date)
        s3_client = get_s3_resource()
        return luigi.contrib.s3.S3Target(
            path=output_path,
            format=luigi.format.Nop,
            client=s3_client
        ), luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "training", f"training-metadata-{self.ingesta}-{self.date}.json")
        )