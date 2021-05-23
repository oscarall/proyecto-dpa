#####################
# FEATURE ENGINEERING
#####################

#Cargando librerías y estableciendo rutas
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split


#PATH = '/home/fernanda/Documents/Projects/data_product_architecture_2021/'
#inspect_df = pd.read_csv(PATH+'Food_Inspections.csv')
#df_clean, metadata_clean = clean_all(inspect_df)
#df_clean.to_pickle(PATH+'clean.pkl', compression='infer', protocol=5, storage_options=None)
#PATH = '/home/fernanda/Documents/Projects/data_product_architecture_2021/'
#df = pd.read_pickle(PATH+"clean.pkl")

def feature_engineering_all(df):
    df = d_cols_inicial(df)
    df = enc_fac_type(df)
    df = enc_zip(df)
    df = enc_risk(df)
    df = enc_insp_type(df)
    df = enc_label(df)
	df = comp_columns(df)
    df_train, df_test = sep_train_test(df)
    df_final = df_train.append(df_test)
    d = {'descripcion': ['registros total encoding',
                         'columnas total encoding',
                         'registros train', 
                         'registros test']
                        , 'valor': [df_final.shape[0],
                                    df_final.shape[1],
                                    df_train.shape[0],
                                    df_test.shape[0]]}
    metadata_fe = pd.DataFrame(data=d) 
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

def comp_columns(df):
    #completando columnas
    list_columns = ['ind_reinspect','violation1','violation10','violation11','violation12','violation13',
                    'violation14','violation15','violation16','violation17','violation18','violation19',
                    'violation2','violation20','violation21','violation22','violation23','violation24',
                    'violation25','violation26','violation27','violation28','violation29','violation3',
                    'violation30','violation31','violation32','violation33','violation34','violation35',
                    'violation36','violation37','violation38','violation39','violation4','violation40',
                    'violation41','violation42','violation43','violation44','violation45','violation46',
                    'violation47','violation48','violation49','violation5','violation50','violation51',
                    'violation52','violation53','violation54','violation55','violation56','violation57',
                    'violation58','violation59','violation6','violation60','violation61','violation62',
                    'violation63','violation64','violation7','violation70','violation8','violation9',
                    'fac_type_bakery','fac_type_banquet hall','fac_type_bar','fac_type_candy store',
                    'fac_type_caterer','fac_type_children services','fac_type_church','fac_type_coffee shop',
                    'fac_type_events','fac_type_fitness','fac_type_gas station','fac_type_goldendiner',
                    'fac_type_grocery store','fac_type_hospital','fac_type_hotel','fac_type_liquor store',
                    'fac_type_mobile food dispenser','fac_type_nursing home','fac_type_paleteria',
                    'fac_type_restaurant','fac_type_school','fac_type_shelter','fac_type_wholesale',
                    'zip_60601.0','zip_60602.0','zip_60603.0','zip_60604.0','zip_60605.0','zip_60606.0',
                    'zip_60607.0','zip_60608.0','zip_60609.0','zip_60610.0','zip_60611.0','zip_60612.0',
                    'zip_60613.0','zip_60614.0','zip_60615.0','zip_60616.0','zip_60617.0','zip_60618.0',
                    'zip_60619.0','zip_60620.0','zip_60621.0','zip_60622.0','zip_60623.0','zip_60624.0',
                    'zip_60625.0','zip_60626.0','zip_60627.0','zip_60628.0','zip_60629.0','zip_60630.0',
                    'zip_60631.0','zip_60632.0','zip_60633.0','zip_60634.0','zip_60636.0','zip_60637.0',
                    'zip_60638.0','zip_60639.0','zip_60640.0','zip_60641.0','zip_60642.0','zip_60643.0',
                    'zip_60644.0','zip_60645.0','zip_60646.0','zip_60647.0','zip_60649.0','zip_60651.0',
                    'zip_60652.0','zip_60653.0','zip_60654.0','zip_60655.0','zip_60656.0','zip_60657.0',
                    'zip_60659.0','zip_60660.0','zip_60661.0','zip_60666.0','zip_60707.0','zip_60827.0',
                    'risk_Risk 1 (High)','risk_Risk 2 (Medium)','risk_Risk 3 (Low)','insp_type_canvass',
                    'insp_type_complaint','insp_type_consultation','insp_type_license',
                    'insp_type_suspected food poisoning','insp_type_task force','col_aux']
    df = df.reindex(df.columns.union(list_columns, sort=False), axis=1, fill_value=0)
    list_columns.append('inspection_id')
    list_columns.append('label')
    df_p = df[list_columns]
    return df_p

def sep_train_test(df_p):
    # separar en train  y test 
    df_train, df_test = train_test_split(df_p ,test_size=0.25, random_state=100, stratify = df_p['label'])
    df_train['ind_train'] = 1
    df_test['ind_train'] = 0
    return df_train, df_test

