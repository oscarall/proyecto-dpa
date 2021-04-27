import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.almacenamiento_test import AlmacenamientoUnitTest


class AlmacenamientoMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return AlmacenamientoUnitTest(ingesta=self.ingesta, date=self.date)
