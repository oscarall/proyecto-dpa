#Cargando librerías y estableciendo rutas
import pandas as pd
import numpy as np
import seaborn as sns

from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
from aequitas.plotting import Plot

def sesgo_inequidad(df_final, transform_final):
    x_df = df_final.drop(['label'], axis=1)
    y_df = df_final['label'].reset_index(drop=True)
    
    y_predict = pd.DataFrame(transform_final.predict(x_df), columns = ['score']).reset_index(drop=True)
    
    ZC = df_final.iloc[:, 83:142] #ZIP Codes
    ZipCode = pd.DataFrame(ZC.idxmax(1), columns = ['zip_code']).reset_index(drop=True) #Reverse One Hot Encoding for ZIP Code
    
    df_aequitas = pd.concat([y_predict, y_df, ZipCode], axis=1)
    df_aequitas.rename(columns={'label': 'label_value'}, inplace=True)
    
    #Aequitas
    g = Group()
    xtab, attrbs = g.get_crosstabs(df_aequitas)
    absolute_metrics = g.list_absolute_metrics(xtab)

    #Bias
    bias = Bias()
    bdf = bias.get_disparity_predefined_groups(xtab, original_df=df_aequitas, 
                                        ref_groups_dict={'zip_code':'zip_60611.0'}, 
                                        alpha=0.05)

    bdf_ = bdf[['attribute_name', 'attribute_value'] +
         bias.list_disparities(bdf)].round(2)

    df_bias = bdf_[['attribute_name', 'attribute_value', 'fdr_disparity', 'fpr_disparity']]

    #Fairness
    fair = Fairness()
    fdf = fair.get_group_value_fairness(bdf)
    parity_determinations = fair.list_parities(fdf)

    fairness = fdf[['attribute_name', 'attribute_value'] + absolute_metrics + 
    parity_determinations].round(2)

    df_fairness = fairness[['fdr','fpr']]
    
    #Bias_Fairness
    df_bias_fairness = pd.concat([df_bias, df_fairness], axis=1)
    
    #Metadata
    metadata_aequitas = { 
        'registros_total': df_aequitas.shape[0],
        'columnas_total': df_aequitas.shape[1],
    }
        
    return df_bias_fairness, metadata_aequitas


