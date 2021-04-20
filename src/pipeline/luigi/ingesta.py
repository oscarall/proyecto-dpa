import luigi
import tempfile
import os
import pickle
import json

from src.pipeline.ingesta_almacenamiento import get_client, ingesta_consecutiva, ingesta_inicial

class Ingesta(luigi.Task):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def run(self):
        client = get_client()

        data = None
        metadata = {
            "id": self.task_id,
            "step": 1
        }

        if self.ingesta == "consecutiva":
            data = ingesta_consecutiva(client, self.date)
        
        if self.ingesta == "inicial":
            data = ingesta_inicial(client, 300000)

        if not data:
            raise Exception(f"Invalid ingesta value: {self.ingesta}")

        metadata["metadata"] = {
            "registros": len(data),
            "tipoIngesta": self.ingesta,
            "fecha": self.date
        }

        with self.output()[0].open("w") as target:
            target.write(pickle.dumps(data))

        with self.output()[1].open("w") as metadata_target:
            metadata_target.write(json.dumps(metadata))

    def output(self):
        return luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "ingesta", f"ingesta-{self.ingesta}-{self.date}.pkl"), 
            format=luigi.format.Nop
        ), luigi.LocalTarget(
            os.path.join(tempfile.gettempdir(), "ingesta", f"ingesta-metadata-{self.ingesta}-{self.date}.json")
        )