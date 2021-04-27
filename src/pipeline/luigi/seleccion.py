import luigi
import tempfile
import os
import pickle
import json
import pandas as pd
import luigi.contrib.s3
import io

from datetime import date
from src.pipeline.luigi.entrenamiento_metadata import EntrenamientoMetadata
from src.pipeline.seleccion import model_select
from src.utils.constants import SELECCION_PATH, BUCKET
from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.utils.task import DPATask

class Seleccion(DPATask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return EntrenamientoMetadata(ingesta=self.ingesta, date=self.date)

    def run(self):
        data = None
        metadata = {
            "id": self.task_id,
            "step": 6
        }

        with self.input()[0].open("r") as input_target:
            data = input_target.read()

        fe_task_input = self.requires().requires().requires().input()
        fe_data = None
        with fe_task_input[0].open("r") as fe_target:
            fe_data = fe_target.read()

        dict_models = pickle.loads(data)
        inspect_df = pd.read_csv(io.StringIO(fe_data))

        select_data, select_metadata = model_select(inspect_df, dict_models)

        metadata["metadata"] = select_metadata.to_dict()

        with self.output()[0].open("w") as target:
            target.write(pickle.dumps(select_data))
        
        with self.output()[1].open("w") as metadata_target:
            metadata_target.write(json.dumps(metadata))


    def output(self):
        file_date = self.date or date.today().strftime("%Y-%m-%d")
        path = SELECCION_PATH
        output_path = "s3://{}/{}/{}-{}.pkl".format(BUCKET, path, self.ingesta, file_date)
        s3_client = get_s3_resource()
        return luigi.contrib.s3.S3Target(
            path=output_path,
            format=luigi.format.Nop,
            client=s3_client
        ), luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "seleccion", f"seleccion-metadata-{self.ingesta}-{self.date}.json")
        )