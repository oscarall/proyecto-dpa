import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.limpieza import Limpieza


class LimpiezaMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return Limpieza(ingesta=self.ingesta, date=self.date)