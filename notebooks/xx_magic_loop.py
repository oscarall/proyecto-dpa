
import pandas as pd
import numpy as np

from sklearn import preprocessing, cross_validation, svm, metrics, tree, decomposition, svm
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression, Perceptron, SGDClassifier, OrthogonalMatchingPursuit, RandomizedLogisticRegression
from sklearn.neighbors.nearest_centroid import NearestCentroid
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.grid_search import ParameterGrid
from sklearn.metrics import *
from sklearn.preprocessing import StandardScaler

import random
import pylab as pl
import matplotlib.pyplot as plt
from scipy import optimize
import time

def define_hyper_params():
    """
        Esta función devuelve un diccionario con
        los clasificadores que vamos a utilizar y
        una rejilla de hiperparámetros
    """
    ## Por ejemplo
    ## classifiers = {
    ##     'RF': RandomForestClassifier(n_estimators=50, n_jobs=-1),
    ##     'NB': GaussianNB(), ...
    ## }

    ## grid = {
    ##     'RF': {'n_estimators': [1,10,100,1000,10000],
    ##            'max_depth': [1,5,10,20,50,100],
    ##            'max_features': ['sqrt','log2'],
    ##            'min_samples_split': [2,5,10]
    ##     },
    ##     'NB': { ... },
    ##     ...
    ## }

    return classifiers, grid

def precision_at_k(y_true, y_scores, k):
    threshold = np.sort(y_scores)[::-1][int(k*len(y_scores))]
    y_pred = np.asarray([1 if i >= threshold else 0 for i in y_scores])
    return metrics.precision_score(y_true, y_pred)

def magic_loop(models_to_run, clfs, grid, X, y):
    for n in range(1, 2):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
        for index, clf in enumerate([clfs[x] for x in models_to_run]):
            logger.debug(models_to_run[index])
            parameter_values = grid[models_to_run[index]]
            for p in ParameterGrid(parameter_values):
                try:
                    clf.set_params(**p)
                    logger.debug(clf)
                    y_pred_probs = clf.fit(X_train, y_train).predict_proba(X_test)[:,1]
                    logger.debug(precision_at_k(y_test,y_pred_probs,.05))
                    #plot_precision_recall_n(y_test,y_pred_probs,clf)
                except IndexError as e:
                    print('Error:', e)
                    continue
