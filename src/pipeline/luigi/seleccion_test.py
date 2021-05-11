import luigi

from src.pipeline.luigi.seleccion import Seleccion
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.seleccion import SeleccionTest

class SeleccionUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    test_case = SeleccionTest
    step = 6

    def requires(self):
        return Seleccion(ingesta=self.ingesta, date=self.date)
