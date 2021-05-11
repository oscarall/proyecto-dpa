import luigi
import tempfile
import os
import pickle
import json
import pandas as pd
import luigi.contrib.s3
import io

from datetime import date
from src.pipeline.luigi.seleccion_metadata import SeleccionMetadata
from src.utils.constants import SESGO_INEQUIDAD_PATH, BUCKET
from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.utils.task import DPATask
from src.pipeline.sesgo_inequidad import sesgo_inequidad

class SesgoInequidad(DPATask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return SeleccionMetadata(ingesta=self.ingesta, date=self.date)

    def run(self):
        data = None
        metadata = {
            "id": self.task_id,
            "step": 7
        }

        with self.input()[0].open("r") as input_target:
            data = input_target.read()

        model = pickle.loads(data)

        fe_task_input = self.requires().requires().requires().requires().requires().requires().input()
        fe_data = None
        with fe_task_input[0].open("r") as fe_target:
            fe_data = fe_target.read()

        inspect_df = pd.read_csv(io.StringIO(fe_data))

        sesgo_inequidad_data, sesgo_inequidad_metadata = sesgo_inequidad(inspect_df, model)

        
        metadata["metadata"] = sesgo_inequidad_metadata

        with self.output()[0].open("w") as target:
            target.write(pickle.dumps(sesgo_inequidad_data))
        
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
            os.path.join(tempfile.gettempdir(), "sesgo-inequidad", f"sesgo-inequidad-metadata-{self.ingesta}-{self.date}.json")
        )