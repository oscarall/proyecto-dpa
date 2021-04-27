import marbles.core
import pickle
import json

class EntrenamientoTest(marbles.core.TestCase):
    data = None
    metadata = None
    force_failure = False

    def test_almacenamiento(self):
        test_data = None
        test_metadata = None
        
        with self.data.open('r') as ingesta_data:
            test_data = pickle.loads(ingesta_data.read())
        
        with self.metadata.open('r') as ingesta_metadata:
            test_metadata = json.loads(ingesta_metadata.read())

        self.assertListEqual(
            list(test_data.keys()), 
            test_metadata["metadata"]["modelos"]
        )

        self.assertFalse(self.force_failure)
