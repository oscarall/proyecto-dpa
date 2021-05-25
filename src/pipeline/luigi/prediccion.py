import luigi
import tempfile
import os
import pickle
import json
import pandas as pd
import luigi.contrib.s3
import io

from datetime import date
from src.pipeline.luigi.seleccion import Seleccion
from src.pipeline.luigi.feature_engineering_metadata import FeatureEngineeringMetadata
from src.pipeline.prediccion import predict
from src.utils.constants import PREDICCION_PATH, BUCKET
from src.pipeline.ingesta_almacenamiento import get_s3_resource

class Prediccion(luigi.Task):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return Seleccion(ingesta=self.ingesta, date=self.date), FeatureEngineeringMetadata(ingesta=self.ingesta, date=self.date)

    def run(self):
        data = None
        metadata = {
            "id": self.task_id,
            "step": 8
        }

        with self.input()[0].open("r") as input_target:
            data = input_target.read()

        fe_input = self.requires()[1].requires().input()[0]

        with fe_input.open("r") as fe_target:
            fe_data = fe_target.read()

        model = pickle.loads(data)
        inspect_df = pd.read_csv(io.StringIO(fe_data))

        predict_data, predict_metadata = predict(inspect_df, model)

        metadata["metadata"] = predict_metadata

        with self.output()[0].open("w") as target:
            target.write(pickle.dumps(predict_data))
        
        with self.output()[1].open("w") as metadata_target:
            metadata_target.write(json.dumps(metadata))


    def output(self):
        file_date = self.date or date.today().strftime("%Y-%m-%d")
        path = PREDICCION_PATH
        output_path = "s3://{}/{}/{}-{}.pkl".format(BUCKET, path, self.ingesta, file_date)
        s3_client = get_s3_resource()
        return luigi.contrib.s3.S3Target(
            path=output_path,
            format=luigi.format.Nop,
            client=s3_client
        ), luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "prediccion", f"prediccion-metadata-{self.ingesta}-{self.date}.json")
        )