import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.entrenamiento_test import EntrenamientoUnitTest


class EntrenamientoMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return EntrenamientoUnitTest(
            ingesta=self.ingesta, 
            date=self.date
        )