#Cargando librerías y estableciendo rutas
import pandas as pd

def predict(df, model_selected):
    x=df.drop('label',axis=1)
    y=df['label']
    trans = model_selected
    y_pred = trans.predict(x)
    df['pred'] = y_pred
    d = {'descripcion': ['registros predichos']
                        , 'valor': [x.shape[0]]}
    metadata_predict = pd.DataFrame(data=d) 
    return df, metadata_predict 