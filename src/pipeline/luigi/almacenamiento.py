import luigi
import luigi.contrib.s3
from datetime import date
from src.pipeline.luigi.ingesta import Ingesta
from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.utils.constants import (
    BUCKET,
    INGESTA_CONSECUTIVA_PATH,
    INGESTA_INICIAL_PATH
)

class Almacenamiento(luigi.Task):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return Ingesta(ingesta=self.ingesta, date=self.date)
    
    def run(self):
        data = None
        with self.input().open("r") as input_target:
            data = input_target.read()

        with self.output().open("w") as target:
            target.write(data)
        
    def output(self):
        file_date = self.date or date.today().strftime("%Y-%m-%d")
        path = INGESTA_CONSECUTIVA_PATH if self.ingesta == "consecutiva" else INGESTA_INICIAL_PATH
        output_path = "s3://{}/{}-{}.pkl".format(BUCKET, path, file_date)
        s3_client = get_s3_resource()
        return luigi.contrib.s3.S3Target(path=output_path, format=luigi.format.Nop, client=s3_client)
