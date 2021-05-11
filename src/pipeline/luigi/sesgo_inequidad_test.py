import luigi
import unittest
import sys

from src.pipeline.luigi.sesgo_inequidad import sesgo_inequidad
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.sesgo_inequidad import sesgo_inequidadTest

class SesgoeInequidadUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    test_case = sesgo_inequidadTest
    step = 8 //revisar con Oscar

    def requires(self):
        return sesgo_inequidad(ingesta=self.ingesta, date=self.date)