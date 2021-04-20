import luigi

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.feature_engineering import FeatureEngineering


class FeatureEngineeringMetadata(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)

    def requires(self):
        return FeatureEngineering(ingesta=self.ingesta, date=self.date)