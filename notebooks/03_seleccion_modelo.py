#Cargando librerías y estableciendo rutas
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn import preprocessing, svm, metrics, tree, decomposition, svm
from sklearn.model_selection import cross_validate
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression, Perceptron, SGDClassifier, OrthogonalMatchingPursuit
from sklearn.neighbors import NearestCentroid
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import ParameterGrid
from sklearn.metrics import *
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

def tabla_select_model(df, dict_modelos):
    df_train = df[df['ind_train']==1]
    df_test = df[df['ind_train']==0]

    x_train=df_train.drop('label',axis=1)
    x_test=df_test.drop('label',axis=1)

    y_train=df_train['label']
    y_test=df_test['label']
    
    listado_modelos = dict_modelos.keys()

    result3 = pd.DataFrame(columns=('score_train', 'score_test'))
    for x in listado_modelos:
            print(x)
            print(dict_modelos[x]['total'])
            trans = dict_modelos[x]['total']
            y_pred_train = trans.predict(x_train)
            y_pred_test = trans.predict(x_test)
            recall_train = trans.score(x_train, y_train)
            recall_test = trans.score(x_test,y_test)
            print(recall_train, recall_test)
            row_to_add = pd.Series({'score_train': recall_train, "score_test": recall_test}, name=x)
            result3 = result3.append(row_to_add)
    return result3 


def model_select(df, dict_modelos):
    result_pre = tabla_select_model(df, dict_modelos)
    modelo_select = result_pre['score_test'].idxmax()
    transform_final = resultados_magic_loop[modelo_select]['total']
    metadata_select = result_pre
    return transform_final, metadata_select