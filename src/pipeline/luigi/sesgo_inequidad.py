import luigi
import tempfile
import os
import pickle
import json
import pandas as pd
import luigi.contrib.s3
import io

from datetime import date
from src.pipeline.luigi.entrenamiento_metadata import EntrenamientoMetadata // REVISAR
from src.pipeline.seleccion import model_select // REVISAR
from src.utils.constants import SELECCION_PATH, BUCKET // REVISAR
from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.utils.task import DPATask

class sesgo_inequidad(DPATask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return sesgo_inequidadMetadata(ingesta=self.ingesta, date=self.date)

    def run(self):
        data = None
        metadata = {
            "id": self.task_id,
            "step": 3 // REVISAR
        }

        with self.input()[0].open("r") as input_target:
            data = input_target.read()

        inspect_df = pd.read_csv(io.StringIO(data))

        sesgo_inequidad_data, sesgo_inequidad_metadata = sesgo_inequidad(inspect_df)

        metadata["metadata"] = inequidad_sesgo_metadata

        with self.output()[0].open("w") as target:
            target.write(sesgo_inequidad_data_csv)
        
        with self.output()[1].open("w") as metadata_target:
            metadata_target.write(json.dumps(metadata))
            
    def output(self):
        file_date = self.date or date.today().strftime("%Y-%m-%d")
        path = SESGO_INEQUIDAD_PATH
        output_path = "s3://{}/{}/{}-{}.pkl".format(BUCKET, path, self.ingesta, file_date)
        s3_client = get_s3_resource()
        return luigi.contrib.s3.S3Target(
            path=output_path,
            format=luigi.format.Nop,
            client=s3_client
        ), luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "sesgo_inequidad", f"sesgo_inequidad-metadata-{self.ingesta}-{self.date}.json")
        )