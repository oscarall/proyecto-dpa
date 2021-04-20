#####################
# FEATURE ENGINEERING
#####################

#Cargando librerías y estableciendo rutas
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split


def feature_engineering_all(df):
    df = d_cols_inicial(df)
    df = enc_fac_type(df)
    df = enc_zip(df)
    df = enc_risk(df)
    df = enc_insp_type(df)
    df = enc_label(df)
    df_train, df_test = sep_train_test(df)
    df_final = df_train.append(df_test)
    metadata_fe = {
        'registros total encoding': df_final.shape[0],
        'columnas total encoding': df_final.shape[1],
        'registros train': df_train.shape[0], 
        'registros test': df_test.shape[0]
    }

    return df_final, metadata_fe

def d_cols_inicial(df):
    df = df.drop(df.filter(regex='descrip').columns, axis=1)
    df = df.drop(df.filter(regex='comment').columns, axis=1)
    df = df.drop(df.filter(regex='name').columns, axis=1)
    df = df.drop(df.filter(regex='license').columns, axis=1)
    df = df.drop(df.filter(regex='address').columns, axis=1)
    df = df.drop(df.filter(regex='date').columns, axis=1)
    df = df.drop(df.filter(regex='latitude').columns, axis=1)
    df = df.drop(df.filter(regex='longitude').columns, axis=1)
    return df

def enc_fac_type(df):
    # encoding facility type
    ohe = OneHotEncoder(handle_unknown='ignore')
    fac_ty_df = pd.DataFrame(ohe.fit_transform(df[['facility_type']]).toarray())
    fac_ty_df.columns = ohe.get_feature_names(['fac_type'])
    fac_ty_df['inspection_id'] = df.inspection_id
    df_p = pd.merge(df  , fac_ty_df, how="left", on=["inspection_id"])
    df_p = df_p.drop(['facility_type'], axis = 1)
    return df_p

def enc_zip(df):
    # encoding zip
    ohe = OneHotEncoder(handle_unknown='ignore')
    zip_df = pd.DataFrame(ohe.fit_transform(df[['zip']]).toarray())
    zip_df.columns = ohe.get_feature_names(['zip'])
    zip_df['inspection_id'] = df.inspection_id
    df_p = pd.merge(df, zip_df, how="left", on=["inspection_id"])
    df_p = df_p.drop(['zip'], axis = 1)
    return df_p

def enc_risk(df):
    # encoding risk
    ohe = OneHotEncoder(handle_unknown='ignore')
    risk_df = pd.DataFrame(ohe.fit_transform(df[['risk']]).toarray())
    risk_df.columns = ohe.get_feature_names(['risk'])
    risk_df['inspection_id'] = df.inspection_id
    df_p = pd.merge(df, risk_df, how="left", on=["inspection_id"])
    df_p = df_p.drop(['risk'], axis = 1)
    return df_p

def enc_insp_type(df):
    # encoding risk
    ohe = OneHotEncoder(handle_unknown='ignore')
    insp_ty_df = pd.DataFrame(ohe.fit_transform(df[['inspection_type']]).toarray())
    insp_ty_df.columns = ohe.get_feature_names(['insp_type'])
    insp_ty_df['inspection_id'] = df.inspection_id
    df_p = pd.merge(df, insp_ty_df, how="left", on=["inspection_id"])
    df_p = df_p.drop(['inspection_type'], axis = 1)
    return df_p

def enc_label(df):
    # encoding label
    ohe = OneHotEncoder(handle_unknown='ignore')
    label_df = pd.DataFrame(ohe.fit_transform(df[['results']]).toarray())
    label_df.columns = ohe.get_feature_names(['label'])
    label_df['inspection_id'] = df.inspection_id
    label_df = label_df.drop('label_pass', axis=1)
    label_df = label_df.rename(columns={'label_fail': 'label'})
    df_p = pd.merge(df, label_df, how="left", on=["inspection_id"])
    df_p = df_p.drop(['results'], axis = 1)
    return df_p

def sep_train_test(df_p):
    # separar en train  y test 
    df_train, df_test = train_test_split(df_p ,test_size=0.25, random_state=100, stratify = df_p['label'])
    df_train['ind_train'] = 1
    df_test['ind_train'] = 0
    return df_train, df_test

