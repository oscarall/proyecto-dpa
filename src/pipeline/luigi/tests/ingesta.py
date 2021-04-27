import marbles.core
import pickle
import json

class IngestaTest(marbles.core.TestCase):
    data = None
    metadata = None

    def test_ingesta_data(self):
        test_data = None
        test_metadata = None
        
        with self.data.open('r') as ingesta_data:
            test_data = pickle.loads(ingesta_data.read())
        
        with self.metadata.open('r') as ingesta_metadata:
            test_metadata = json.loads(ingesta_metadata.read())

        self.assertEqual(
            test_metadata["metadata"]["registros"],
            len(test_data)
        )
        self.assertIsInstance(test_data, list)
        

