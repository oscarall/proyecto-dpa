import marbles.core
import pickle
import json
import pandas as pd
import io

class SesgoInequidadTest(marbles.core.TestCase):
    data = None
    metadata = None

    def test_sesgo_inequidad(self):
        test_data = None
        test_metadata = None
        
        with self.data.open('r') as ingesta_data:
            test_data = ingesta_data.read()
        
        with self.metadata.open('r') as ingesta_metadata:
            test_metadata = json.loads(ingesta_metadata.read())

        inspect_df = pd.read_csv(io.StringIO(test_data))
        
        self.assertEqual(
            test_metadata["metadata"]["columnas_total"], 
            len(inspect_df.columns)-1
        )
        
        self.assertEqual(
            test_metadata["metadata"]["registros_total"], 
            len(inspect_df.index)
        )       