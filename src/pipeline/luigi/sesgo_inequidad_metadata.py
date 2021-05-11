import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.sesgo_inequidad_test import SesgoeInequidadUnitTest


class SesgoInequidadMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return SesgoeInequidadUnitTest(ingesta=self.ingesta, date=self.date)
