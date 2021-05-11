import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.sesgo_inequidad_test import sesgo_inequidadUnitTest


class sesgo_inequidadMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return sesgo_inequidadUnitTest(ingesta=self.ingesta, date=self.date)
