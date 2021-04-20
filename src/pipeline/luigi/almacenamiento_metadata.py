import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.almacenamiento import Almacenamiento


class AlmacenamientoMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return Almacenamiento(ingesta=self.ingesta, date=self.date)
