import marbles.core
import pickle
import json
import pandas as pd
import io


class PrediccionTest(marbles.core.TestCase):
    data = None
    metadata = None

    def test_limpieza(self):
        test_data = None
        test_metadata = None
        
        with self.data.open('r') as ingesta_data:
            test_data = ingesta_data.read()
        
        with self.metadata.open('r') as ingesta_metadata:
            test_metadata = json.loads(ingesta_metadata.read())

        predictions = pickle.loads(test_data)

        self.assertEquals(
            predictions.shape[0], 
            test_metadata["metadata"]["registros predichos"], 
            "Hay diferencia en el número de registros"
        )
        
        
        