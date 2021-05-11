import luigi

from src.pipeline.luigi.sesgo_inequidad import SesgoInequidad
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.sesgo_inequidad import SesgoInequidadTest

class SesgoeInequidadUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    test_case = SesgoInequidadTest
    step = 7

    def requires(self):
        return SesgoInequidad(ingesta=self.ingesta, date=self.date)