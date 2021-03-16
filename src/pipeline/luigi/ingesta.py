import luigi
import tempfile
import os

from src.pipeline.ingesta_almacenamiento import get_client, ingesta_consecutiva, ingesta_inicial

class Ingesta(luigi.Task):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def run(self):
        client = get_client()

        data = None

        if self.ingesta == "consecutiva":
            data = ingesta_consecutiva(client, self.date)
        
        if self.ingesta == "inicial":
            data = ingesta_inicial(client, 300000)

        if not data:
            raise Exception(f"Invalid ingesta value: {self.ingesta}")

        with self.output().open("w") as target:
            target.write(data)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "ingesta", f"ingesta-{self.ingesta}-{self.date}.pkl"), 
            format=luigi.format.Nop
        )