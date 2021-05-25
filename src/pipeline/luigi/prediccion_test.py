import luigi

from src.pipeline.luigi.prediccion import Prediccion
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.prediccion import PrediccionTest

class PrediccionUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    test_case = PrediccionTest
    step = 8

    def requires(self):
        return Prediccion(ingesta=self.ingesta, date=self.date)
