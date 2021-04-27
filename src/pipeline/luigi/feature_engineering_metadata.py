import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.feature_engineering_test import FeatureEngineeringUnitTest


class FeatureEngineeringMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return FeatureEngineeringUnitTest(ingesta=self.ingesta, date=self.date)