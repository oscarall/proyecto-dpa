import luigi
import unittest
import sys

from src.pipeline.luigi.entrenamiento import Entrenamiento
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.entrenamiento import EntrenamientoTest

class EntrenamientoUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    test_case = EntrenamientoTest
    step = 5

    def requires(self):
        return Entrenamiento(ingesta=self.ingesta, date=self.date)
