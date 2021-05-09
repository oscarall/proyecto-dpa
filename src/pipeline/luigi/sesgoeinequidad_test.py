import luigi
import unittest
import sys

from src.pipeline.luigi.sesgoeinequidad import SesgoeInequidad
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.sesgoeinequidad import SesgoeInequidadTest

class SesgoeInequidadUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    test_case = SesgoeInequidadTest
    step = 8 //revisar con Oscar

    def requires(self):
        return SesgoeInequidad(ingesta=self.ingesta, date=self.date)