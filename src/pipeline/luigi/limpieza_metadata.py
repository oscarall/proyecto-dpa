import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.limpieza_test import LimpiezaUnitTest


class LimpiezaMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return LimpiezaUnitTest(ingesta=self.ingesta, date=self.date)