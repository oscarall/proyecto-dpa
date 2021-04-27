import luigi
import unittest
import sys

from src.pipeline.luigi.ingesta import Ingesta
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.ingesta import IngestaTest

class IngestaUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)
    test_case = IngestaTest
    step = 1

    def requires(self):
        return Ingesta(ingesta=self.ingesta, date=self.date)


    