import marbles.core
import pickle
import json
import pandas as pd
import io

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression
from sklearn import svm
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier

MODELS = {
    'RF': RandomForestClassifier,
    'AB': AdaBoostClassifier,
    'LR': LogisticRegression,
    'SVM': svm.SVC,
    'GB': GradientBoostingClassifier,
    'NB': GaussianNB,
    'DT': DecisionTreeClassifier,
    'KNN': KNeighborsClassifier 
}

class SeleccionTest(marbles.core.TestCase):
    data = None
    metadata = None

    def test_limpieza(self):
        test_data = None
        test_metadata = None
        
        with self.data.open('r') as ingesta_data:
            test_data = ingesta_data.read()
        
        with self.metadata.open('r') as ingesta_metadata:
            test_metadata = json.loads(ingesta_metadata.read())

        model = pickle.loads(test_data)

        selected_model = None
        winner_score = 0

        for trained_model, score in test_metadata["metadata"]["score_train"].items():
            if score > winner_score:
                selected_model = trained_model
                winner_score = score

        self.assertIsInstance(model.estimator, MODELS[selected_model])
        
        
        