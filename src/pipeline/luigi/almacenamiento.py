import luigi
import os
import tempfile
import json
import luigi.contrib.s3
from datetime import date
from src.pipeline.luigi.ingesta import Ingesta
from src.pipeline.luigi.ingesta_metadata import IngestaMetadata
from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.utils.constants import (
    BUCKET,
    INGESTA_CONSECUTIVA_PATH,
    INGESTA_INICIAL_PATH
)
from src.utils.task import DPATask

class Almacenamiento(DPATask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return IngestaMetadata(ingesta=self.ingesta, date=self.date)
    
    def run(self):
        data = None
        metadata = {
            "id": self.task_id,
            "step": 2
        }

        with self.input()[0].open("r") as input_target:
            data = input_target.read()

        outputs = self.output()

        with outputs[0].open("w") as target:
            target.write(data)

        metadata["metadata"] = {
            "path": outputs[0].path,
            "fecha": self.date,
            "tipoIngesta": self.ingesta
        }

        with self.output()[1].open("w") as metadata_target:
            metadata_target.write(json.dumps(metadata))
        
    def output(self):
        file_date = self.date or date.today().strftime("%Y-%m-%d")
        path = INGESTA_CONSECUTIVA_PATH if self.ingesta == "consecutiva" else INGESTA_INICIAL_PATH
        output_path = "s3://{}/{}-{}.pkl".format(BUCKET, path, file_date)
        s3_client = get_s3_resource()
        return luigi.contrib.s3.S3Target(
            path=output_path,
            format=luigi.format.Nop,
            client=s3_client
        ), luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "almacenamiento", f"almacenamiento-metadata-{self.ingesta}-{self.date}.json")
        )
