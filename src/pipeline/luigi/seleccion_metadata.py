import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.seleccion_test import SeleccionUnitTest


class SeleccionMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return SeleccionUnitTest(
            ingesta=self.ingesta, 
            date=self.date
        )