#Cargando librerías y estableciendo rutas
import pandas as pd

def predict(df, model_selected):
    print(df.columns)
    x=df.drop('label',axis=1)
    y=df['label']
    trans = model_selected
    y_pred = trans.predict(x)
    df['pred'] = y_pred
    metadata_predict = {'registros predichos': x.shape[0]}
    return df, metadata_predict 