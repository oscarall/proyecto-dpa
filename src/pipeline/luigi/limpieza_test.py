import luigi
import unittest
import sys

from src.pipeline.luigi.limpieza import Limpieza
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.limpieza import LimpiezaTest

class LimpiezaUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    test_case = LimpiezaTest
    step = 3

    def requires(self):
        return Limpieza(ingesta=self.ingesta, date=self.date)