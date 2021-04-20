################
# DATA CLEANING
################

#Cargando librerías y estableciendo rutas
import pandas as pd
import numpy as np
from datetime import datetime


#PATH = '/home/fernanda/Documents/Projects/data_product_architecture_2021/'
#inspect_df = pd.read_csv(PATH+'Food_Inspections.csv')
#df_clean, metadata_clean = clean_all(inspect_df)

def clean_all(inspect_df):
    inspect_df = inspect_df.rename({"license_": "license"}, axis="columns")
    inspect_df, num_obs_ini          = clean_pre(inspect_df)
    inspect_df, num_obs_elim_dupid   = clean_duplicados(inspect_df)
    inspect_df, num_obs_elim_na      = clean_missing_num(inspect_df)
    inspect_df, num_obs_elim_state   = clean_state(inspect_df)
    inspect_df, num_obs_elim_risk    = clean_risk(inspect_df)
    inspect_df, num_obs_elim_insptyp = clean_inspection_type(inspect_df)
    inspect_df, num_obs_elim_factype = clean_facility_type(inspect_df)
    inspect_df, num_obs_elim_city    = clean_city(inspect_df)
    inspect_df, num_obs_elim_results = clean_results(inspect_df)
    inspect_df                       = clean_violations(inspect_df)
    inspect_df, num_obs_final        = clean_final(inspect_df)
    metadata_clean = {
        'registros iniciales': num_obs_ini,
        'registros sin duplicados id': num_obs_elim_dupid, 
        'registros sin na vars numericas': num_obs_elim_na, 
        'registros sin error estado': num_obs_elim_state, 
        'registros sin error risk': num_obs_elim_risk,  
        'registros sin error inspection type': num_obs_elim_insptyp,  
        'registros sin error facility type': num_obs_elim_factype,
        'registros sin error city': num_obs_elim_city,
        'registros sin error results': num_obs_elim_results,
        'registros final': num_obs_final
    }

    df_clean = inspect_df
    return df_clean, metadata_clean 

def clean_pre(inspect_df):
    # filtramos 2020 y 2021 para probar 
    #inspect_df = inspect_df.loc[lambda inspect_df: inspect_df['Inspection Date'].str.contains("202")] 
    #inspect_df = inspect_df[inspect_df['Inspection Date'].str.contains("202")]
    # número de registros
    num_obs_ini = inspect_df.shape[0]
    # eliminar "#"
    inspect_df.columns = inspect_df.columns.str.replace('#','',regex=False)
    # Elimina espacios alrededor
    inspect_df.columns = inspect_df.columns.str.strip() 
    # Cambia espacios por "_"
    inspect_df.columns = inspect_df.columns.str.replace(' ','_',regex=False)
    # Cambiar a minúsculas
    inspect_df.columns = inspect_df.columns.str.lower()
    # Pasamos la columna a fecha
    inspect_df.loc['inspection_date_f'] = pd.to_datetime(inspect_df['inspection_date'])
    return inspect_df, num_obs_ini

def clean_duplicados(inspect_df):
    # Eliminamos duplicados de ID 
    inspect_df.drop_duplicates("inspection_id", inplace=True)
    num_obs_elim_dupid = inspect_df.shape[0]
    return inspect_df, num_obs_elim_dupid

def clean_missing_num(inspect_df):
    #eliminamos registros numericos con missing
    inspect_df.dropna(subset=["inspection_id", "license", "zip", "inspection_date", "latitude", "longitude"], inplace=True)
    num_obs_elim_na = inspect_df.shape[0]
    return inspect_df, num_obs_elim_na

def clean_state(inspect_df):
    # eliminamos los casos con uno estado distinto a illinois
    inspect_df = inspect_df[(inspect_df.state == "IL")]
    num_obs_elim_state = inspect_df.shape[0]
    return inspect_df, num_obs_elim_state

def clean_risk(inspect_df):
    # eliminamos tambien los casos sin risk válido
    inspect_df =  inspect_df[inspect_df['risk'].isin(['Risk 1 (High)', 'Risk 2 (Medium)', 'Risk 3 (Low)'])]
    num_obs_elim_risk = inspect_df.shape[0]
    return inspect_df, num_obs_elim_risk

def clean_inspection_type(inspect_df):
    # de acuerdo con la informacion del dataset la variable 'inspection_type' deberia
    # tener uno de los siguientes valores:
    # 'Canvass', 'Consultation', 'Complaint', License' , 'Suspected Food Poisoning', 'Task Force'
    # identificamos que entre los más frecuentes hay re-inspection o reinspection ó re inspection
    # crearemos una variable que contenga este indicador
    # primero a minúsculas
    inspect_df['inspection_type'] = inspect_df['inspection_type'].str.lower()
    # quitamos caracteres especiales y espacios para analizar
    inspect_df['inspection_type']  = inspect_df['inspection_type'].str.replace(' ','',regex=False )
    inspect_df['inspection_type']  = inspect_df['inspection_type'].str.replace('-','',regex=False )
    inspect_df['inspection_type']  = inspect_df['inspection_type'].str.replace('.','',regex=False )
    inspect_df['inspection_type']  = inspect_df['inspection_type'].str.replace('/','',regex=False )
    inspect_df['inspection_type']  = inspect_df['inspection_type'].str.replace('(','',regex=False )
    inspect_df['inspection_type']  = inspect_df['inspection_type'].str.replace(')','',regex=False )
    # creamos la columna 'ind_reinspect' que sera = 1 cuando exista una reinspeccion
    inspect_df['ind_reinspect'] = np.where(inspect_df.inspection_type.str.contains('reinspection'), 1,0)
    # creamos otra columna que agrupe los valores válidos
    inspect_df['inspection_type_aux'] = np.where(inspect_df.inspection_type.str.contains('canvas'), 'canvass',
                                        np.where(inspect_df.inspection_type.str.contains('license'), 'license',
                                        np.where(inspect_df.inspection_type.str.contains('complaint'), 'complaint',
                                        np.where(inspect_df.inspection_type.str.contains('consultation'), 'consultation',
                                        np.where(inspect_df.inspection_type.str.contains('foodpoisoning'), 'suspected food poisoning',
                                        np.where(inspect_df.inspection_type.str.contains('spf'), 'suspected food poisoning',
                                        np.where(inspect_df.inspection_type.str.contains('taskforce'), 'task force'
                                                 ,'others')))))))
    # primero a minúsculas
    inspect_df['city'] = inspect_df['city'].str.lower()
    # quitamos caracteres especiales y espacios para analizar
    inspect_df['city']  = inspect_df['city'].str.replace(' ','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace("'",'',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace(' ','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace("'",'',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace('-','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace('.','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace('/','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace('(','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace(')','',regex=False )
    # creamos otra columna que agrupe los valores válidos
    inspect_df['city'] = np.where(inspect_df.city.str.contains('chicago'), 'chicago',
                                        np.where(inspect_df.city.str.contains('icago'), 'chicago',
                                                 'other'))
    inspect_df = inspect_df[inspect_df['inspection_type_aux'] != 'others']
    num_obs_elim_insptyp = inspect_df.shape[0]
    return inspect_df, num_obs_elim_insptyp

def clean_facility_type(inspect_df):
    # de acuerdo a la informacion del dataset los valores validos para type of facility son
    # bakery, banquet hall, candy store, caterer, coffee shop, day care center (for ages less than 2), day care center (for ages 2 – 6)
    #, day care center (combo, for ages less than 2 and 2 – 6 combined), gas station, Golden Diner, grocery store
    #, hospital, long term care center(nursing home), liquor store, mobile food dispenser, restaurant, paleteria
    #, school, shelter, tavern, social club, wholesaler, or Wrigley Field Rooftop

    # primero a minúsculas
    inspect_df['facility_type'] = inspect_df['facility_type'].str.lower()
    # quitamos caracteres especiales y espacios para analizar
    inspect_df['facility_type']  = inspect_df['facility_type'].str.replace(' ','',regex=False )
    inspect_df['facility_type']  = inspect_df['facility_type'].str.replace("'",'',regex=False )
    inspect_df['facility_type']  = inspect_df['facility_type'].str.replace('-','',regex=False )
    inspect_df['facility_type']  = inspect_df['facility_type'].str.replace('.','',regex=False )
    inspect_df['facility_type']  = inspect_df['facility_type'].str.replace('/','',regex=False )
    inspect_df['facility_type']  = inspect_df['facility_type'].str.replace('(','',regex=False )
    inspect_df['facility_type']  = inspect_df['facility_type'].str.replace(')','',regex=False )
    # creamos otra columna que agrupe los valores válidos
    inspect_df['facility_type_aux'] = np.where(inspect_df.facility_type.str.contains('restaurant'), 'restaurant',
                                        np.where(inspect_df.facility_type.str.contains('goldendiner'), 'goldendiner',
                                        np.where(inspect_df.facility_type.str.contains('grocery'), 'grocery store',
                                        np.where(inspect_df.facility_type.str.contains('foodstore'), 'grocery store',
                                        np.where(inspect_df.facility_type.str.contains('convenien'), 'grocery store',
                                        np.where(inspect_df.facility_type.str.contains('school'), 'school',
                                        np.where(inspect_df.facility_type.str.contains('college'), 'school',
                                        np.where(inspect_df.facility_type.str.contains('liquor'), 'liquor store',
                                        np.where(inspect_df.facility_type.str.contains('hospital'), 'hospital',
                                        np.where(inspect_df.facility_type.str.contains('nursing'), 'nursing home',
                                        np.where(inspect_df.facility_type.str.contains('adult'), 'nursing home',
                                        np.where(inspect_df.facility_type.str.contains('senior'), 'nursing home',
                                        np.where(inspect_df.facility_type.str.contains('assisted'), 'nursing home',
                                        np.where(inspect_df.facility_type.str.contains('assissted'), 'nursing home',
                                        np.where(inspect_df.facility_type.str.contains('asisted'), 'nursing home',
                                        np.where(inspect_df.facility_type.str.contains('supportive'), 'nursing home',
                                        np.where(inspect_df.facility_type.str.contains('longtermcare'), 'nursing home',
                                        np.where(inspect_df.facility_type.str.contains('child'), 'children services',
                                        np.where(inspect_df.facility_type.str.contains('daycare'), 'children services',
                                        np.where(inspect_df.facility_type.str.contains('bake'), 'bakery',
                                        np.where(inspect_df.facility_type.str.contains('banquet'), 'banquet hall',
                                        np.where(inspect_df.facility_type.str.contains('cafe'), 'coffee shop',
                                        np.where(inspect_df.facility_type.str.contains('coffee'), 'coffee shop',
                                        np.where(inspect_df.facility_type.str.contains('candy'), 'candy store',
                                        np.where(inspect_df.facility_type.str.contains('cater'), 'caterer',
                                        np.where(inspect_df.facility_type.str.contains('commissa'), 'caterer',
                                        np.where(inspect_df.facility_type.str.contains('kitchen'), 'caterer',
                                        np.where(inspect_df.facility_type.str.contains('gasstation'), 'gas station',
                                        np.where(inspect_df.facility_type.str.contains('bar'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('brewery'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('club'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('rooftop'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('patio'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('pub'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('tavern'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('wine'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('beer'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('lounge'), 'bar',
                                        np.where(inspect_df.facility_type.str.contains('truck'), 'mobile food dispenser',
                                        np.where(inspect_df.facility_type.str.contains('mobile'), 'mobile food dispenser',
                                        np.where(inspect_df.facility_type.str.contains('cart'), 'mobile food dispenser',
                                        np.where(inspect_df.facility_type.str.contains('motorized'), 'mobile food dispenser',
                                        np.where(inspect_df.facility_type.str.contains('popup'), 'mobile food dispenser',
                                        np.where(inspect_df.facility_type.str.contains('kiosk'), 'mobile food dispenser',
                                        np.where(inspect_df.facility_type.str.contains('stand'), 'mobile food dispenser',
                                        np.where(inspect_df.facility_type.str.contains('station'), 'mobile food dispenser',
                                        np.where(inspect_df.facility_type.str.contains('store'), 'wholesale',
                                        np.where(inspect_df.facility_type.str.contains('market'), 'wholesale',
                                        np.where(inspect_df.facility_type.str.contains('shop'), 'wholesale',
                                        np.where(inspect_df.facility_type.str.contains('wholesale'), 'wholesale',
                                        np.where(inspect_df.facility_type.str.contains('shelter'), 'shelter',
                                        np.where(inspect_df.facility_type.str.contains('special'), 'events',
                                        np.where(inspect_df.facility_type.str.contains('stadium'), 'events',
                                        np.where(inspect_df.facility_type.str.contains('theat'), 'events',
                                        np.where(inspect_df.facility_type.str.contains('art'), 'events',
                                        np.where(inspect_df.facility_type.str.contains('event'), 'events',
                                        np.where(inspect_df.facility_type.str.contains('hotel'), 'hotel',
                                        np.where(inspect_df.facility_type.str.contains('roomserv'), 'hotel',
                                        np.where(inspect_df.facility_type.str.contains('fitness'), 'fitness',
                                        np.where(inspect_df.facility_type.str.contains('herb'), 'fitness',
                                        np.where(inspect_df.facility_type.str.contains('paleteria'), 'paleteria',
                                        np.where(inspect_df.facility_type.str.contains('church'), 'church',
                                        np.where(inspect_df.facility_type.str.contains('religi'), 'church',
                                                 'others')))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
    inspect_df = inspect_df[inspect_df['facility_type_aux'] != 'others']
    num_obs_elim_factype = inspect_df.shape[0] 
    return inspect_df, num_obs_elim_factype

def clean_city(inspect_df):
    # primero a minúsculas
    inspect_df['city'] = inspect_df['city'].str.lower()
    # quitamos caracteres especiales y espacios para analizar
    inspect_df['city']  = inspect_df['city'].str.replace(' ','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace("'",'',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace(' ','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace("'",'',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace('-','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace('.','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace('/','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace('(','',regex=False )
    inspect_df['city']  = inspect_df['city'].str.replace(')','',regex=False )
    # creamos otra columna que agrupe los valores válidos
    inspect_df['city'] = np.where(inspect_df.city.str.contains('chicago'), 'chicago',
                                        np.where(inspect_df.city.str.contains('icago'), 'chicago',
                                                 'other'))
    # eliminamos other
    inspect_df = inspect_df[inspect_df['city'] != 'other']
    num_obs_elim_city = inspect_df.shape[0]
    return inspect_df, num_obs_elim_city

def clean_results(inspect_df):
    # primero a minúsculas
    inspect_df['results'] = inspect_df['results'].str.lower()
    # creamos otra columna que agrupe los valores válidos
    inspect_df['results'] = np.where(inspect_df.results.str.contains('pass'), 'pass',
                            np.where(inspect_df.results.str.contains('fail'), 'fail',
                                     'other'))
    inspect_df = inspect_df[inspect_df['results'] != 'other']
    num_obs_elim_results = inspect_df.shape[0]
    return inspect_df, num_obs_elim_results

def clean_violations(inspect_df):
    inspect_vind = inspect_df.violations.apply(violations_ind).fillna(0)
    inspect_vind['inspection_id'] = inspect_df.inspection_id
    inspect_df_p1 = pd.merge(inspect_df, inspect_vind, how="left", on=["inspection_id"])
    return inspect_df_p1

def clean_final(inspect_df):
    inspect_df = inspect_df.drop(['location', 'facility_type','inspection_type', 'state','city','violations'], axis = 1)
    inspect_df = inspect_df.rename(columns={'facility_type_aux': 'facility_type', 'inspection_type_aux': 'inspection_type'})
    num_obs_final = inspect_df.shape[0]
    return inspect_df, num_obs_final


# tratamiento de violations

def violations_ind(var):
    values_row = pd.Series([],dtype = 'float64')
    if type(var) == str:
        var2 = var.split(' | ')
        for valor in var2:
            index = "violation" + valor.split('.')[0]
            values_row[index] = 1
    return values_row

def violations_desc(var):
    values_row = pd.Series([], dtype = 'object')
    if type(var) == str:
        var2 = var.split(' | ')
        for valor in var2:
            index = "descrip" + valor.split('.')[0]
            descrip = valor.split('.')[1].split('Comments:')[0]
            values_row[index] = descrip
    return values_row

def violations_comment_f(var):
    values_row = pd.Series([], dtype = 'object')
    if type(var) == str:
        var2 = var.split(' | ')
        for valor in var2:
            index = "comment" + valor.split('.')[0]
            descrip_pre = valor.split('.')[1].split('Comments:')
            if len(descrip_pre) == 2:
                values_row[index] = descrip_pre[1]
    return values_row