import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.ingesta import Ingesta


class IngestaMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return Ingesta(ingesta=self.ingesta, date=self.date)
