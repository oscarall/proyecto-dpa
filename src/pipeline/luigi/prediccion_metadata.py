import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.prediccion_test import PrediccionUnitTest


class PrediccionMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return PrediccionUnitTest(ingesta=self.ingesta, date=self.date)