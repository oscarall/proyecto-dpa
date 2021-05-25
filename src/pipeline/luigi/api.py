import luigi
import pickle
import io
import pandas as pd
from pandas.core import api

from src.utils.copytotable import DPACopyToTable
from src.pipeline.luigi.prediccion_metadata import PrediccionMetadata

class Api(DPACopyToTable):
    ingesta = luigi.Parameter(default="consecutiva")
    date = luigi.Parameter(default=None)
    table = 'api.scores'
    columns = ['inspection_id', 'name', 'risk', 'address', 'zip_code', 'inspection_date', 'predicted']

    def requires(self):
        return PrediccionMetadata(ingesta=self.ingesta, date=self.date)

    def get_limpieza_df(self):
        limpieza_target = self.requires() \
            .requires() \
            .requires() \
            .requires()[1] \
            .requires() \
            .requires() \
            .input()[0]

        with limpieza_target.open("r") as input_target:
            data = input_target.read()

        df = pd.read_csv(io.StringIO(data))
        df = df[["inspection_id", "dba_name", "risk", "address", "zip", "inspection_date"]]
        return df

    def get_predictions_df(self):
        predictions_target = self.requires().requires().input()[0]

        with predictions_target.open("r") as input_target:
            data = input_target.read()
        
        df = pickle.loads(data)
        df = df[["inspection_id", "pred"]]

        df["pred"] = df["pred"].map(lambda x: "fail" if x == 1.0 else "pass")

        return df

    def get_values(self):
        predictions_df = self.get_predictions_df()
        limpieza_df = self.get_limpieza_df()

        api_df = pd.merge(limpieza_df, predictions_df, on='inspection_id')
        api_dict = api_df.to_dict(orient='records')

        api_values = list(map(lambda inspections: tuple(inspections.values()), api_dict))

        return api_values

    def rows(self):
        values = self.get_values()

        for value in values:
            yield value
    
    