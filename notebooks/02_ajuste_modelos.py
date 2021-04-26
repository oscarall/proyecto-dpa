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


def define_clfs_params():

    clfs = {'RF': RandomForestClassifier(random_state=0),
        'AB': AdaBoostClassifier(DecisionTreeClassifier(max_depth=1), algorithm="SAMME", n_estimators=200,random_state=0),
        'LR': LogisticRegression(random_state=0),
        'SVM': svm.SVC(probability=True, random_state=0),
        'GB': GradientBoostingClassifier(random_state=0),
        'NB': GaussianNB(),
        'DT': DecisionTreeClassifier(random_state=0),
        'KNN': KNeighborsClassifier(n_neighbors=3) 
            }

    grid = { 
    'RF':{'n_estimators': [10,100], 'max_depth': [2,5], 'max_features': [0.1, 0.2],'min_samples_split': [2,5]},
    'LR': { 'penalty': ['l2'], 'C': [0.00001, 0.0001,0.001,0.01,0.1,1,2,5,7,10,20]},
    'AB': { 'algorithm': ['SAMME', 'SAMME.R'], 'n_estimators': [1,2,10,50,100]},
    'GB': {'n_estimators': [1,10], 'learning_rate' : [0.001,0.1,0.5],'subsample' : [0.1,0.5], 'max_depth': [5,10]},
    'NB' : {},
    'SVM' :{'C' :[0.001,0.1,10],'kernel':['linear']},
    'KNN' :{'n_neighbors': [5,10,25],'weights': ['uniform','distance'],'algorithm': ['auto','kd_tree']}
           }
    return clfs, grid



def clf_loop(models_to_run, clfs, grid, X_train, X_test, y_train,y_test, search):
    diccionario=dict()
    for n in range(1, 2):
        for index,clf in enumerate([clfs[x] for x in models_to_run]):
            parameter_values = grid[models_to_run[index]] #diccionario de parametros
            #En esta parte debemos usas parameter_values sobre el grid o randomSearch
            #asi veremos cual es la mejor prediccion del modelo

            try:
                if(search == 'random'):
                    print(models_to_run[index])
                    print('MODELO: {}'.format(clf))
                    print('PARAMETER: {}'.format(parameter_values))
                    random_search = RandomizedSearchCV(clf, parameter_values)
                    rand_ent= random_search.fit(X_train, y_train)
                    rand_ent.best_params_
                    rand_ent.best_score_
                    rand_ent.best_estimator_
                    diccionario[models_to_run[index]]=dict()
                    diccionario[models_to_run[index]]['estimator']=rand_ent.best_estimator_
                    diccionario[models_to_run[index]]['score']=rand_ent.best_score_
                    diccionario[models_to_run[index]]['parametros']=rand_ent.best_params_
                    diccionario[models_to_run[index]]['total']= rand_ent
                    print('BEST ESTIMATOR: {}'.format( random_search.best_estimator_))


                elif(search=='grid'):
                    print(models_to_run[index])
                    print('MODELO: {}'.format(clf))
                    print('PARAMETER: {}'.format(parameter_values))
                    grid_search = GridSearchCV(clf, parameter_values, cv = 5)
                    gri_ent = grid_search.fit(X_train, y_train)
                    gri_ent.best_params_
                    gri_ent.best_score_
                    gri_ent.best_estimator_
                    diccionario[models_to_run[index]]=dict()
                    diccionario[models_to_run[index]]['estimator']=gri_ent.best_estimator_
                    diccionario[models_to_run[index]]['score']=gri_ent.best_score_
                    diccionario[models_to_run[index]]['parametros']=gri_ent.best_params_
                    diccionario[models_to_run[index]]['total']= gri_ent
                    print('BEST ESTIMATOR: {}'.format( grid_search.best_estimator_)) 
                else:
                    print('search incorrecto')

            except Exception as e:
                print('Error:',e)
                continue
    return(diccionario)


def run_magic_loop(df):

    df_train = df[df['ind_train']==1]
    df_test = df[df['ind_train']==0]

    x_train=df_train.drop('label',axis=1)
    x_test=df_test.drop('label',axis=1)

    y_train=df_train['label']
    y_test=df_test['label']
    
    from timeit import default_timer as timer
    clfs,grid=define_clfs_params()
    model_to_run=['RF','LR']
    start = timer()
    dict_models = clf_loop(model_to_run, clfs, grid,x_train, x_test, y_train,y_test,  'random')
    end=timer()
    tiempo = end-start
    d = {'descripcion': ['registros train',
                         'registros test', 
                         'tiempo',
                         'modelos']
                        , 'valor': [x_train.shape[0],
                                    x_test.shape[0],
                                    tiempo,
                                    model_to_run]}
    metadata_ml = pd.DataFrame(data=d) 
    return dict_models, metadata_ml