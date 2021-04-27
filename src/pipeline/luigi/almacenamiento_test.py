import luigi
import unittest
import sys

from src.pipeline.luigi.almacenamiento import Almacenamiento
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.almacenamiento import AlmacenamientoTest

class AlmacenamientoUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    test_case = AlmacenamientoTest
    step = 2

    def requires(self):
        return Almacenamiento(ingesta=self.ingesta, date=self.date)