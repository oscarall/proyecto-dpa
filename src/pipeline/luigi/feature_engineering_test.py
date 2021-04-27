import luigi
import unittest
import sys

from src.pipeline.luigi.feature_engineering import FeatureEngineering
from src.utils.test_task import DPATestTask
from src.pipeline.luigi.tests.feature_engineering import FeatureEngineeringTest

class FeatureEngineeringUnitTest(DPATestTask):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    test_case = FeatureEngineeringTest
    step = 4

    def requires(self):
        return FeatureEngineering(ingesta=self.ingesta, date=self.date)