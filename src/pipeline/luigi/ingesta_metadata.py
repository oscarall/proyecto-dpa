import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.ingesta_test import IngestaUnitTest


class IngestaMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return IngestaUnitTest(ingesta=self.ingesta, date=self.date)
