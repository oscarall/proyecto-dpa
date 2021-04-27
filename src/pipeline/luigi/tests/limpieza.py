import marbles.core
import pickle
import json
import pandas as pd
import io

class LimpiezaTest(marbles.core.TestCase):
    data = None
    metadata = None

    def test_limpieza(self):
        test_data = None
        test_metadata = None
        
        with self.data.open('r') as ingesta_data:
            test_data = ingesta_data.read()
        
        with self.metadata.open('r') as ingesta_metadata:
            test_metadata = json.loads(ingesta_metadata.read())

        inspect_df = pd.read_csv(io.StringIO(test_data))
        missing_columns = ['location', 'facility_type','inspection_type', 'state','city','violations']

        self.assertEqual(len(inspect_df["inspection_id"].unique()), len(inspect_df.index))

        self.assertFalse(set(missing_columns).issubset(inspect_df.columns))

        self.assertEqual(len(inspect_df.index), test_metadata["metadata"]["registros final"])
        
        