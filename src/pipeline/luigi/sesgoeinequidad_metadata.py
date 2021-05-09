import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.sesgoeinequidad_test import SesgoeInequidadUnitTest


class SesgoeInequidadMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return SesgoeInequidadUnitTest(ingesta=self.ingesta, date=self.date)
