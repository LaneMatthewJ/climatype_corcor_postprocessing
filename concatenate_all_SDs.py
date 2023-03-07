import os
import pandas as pd
import numpy as np

dirlist = [
"reformatted_merged_combined_global_yearly_2w_1960_top47k",
"reformatted_merged_combined_global_yearly_2w_1970_top47k",
"reformatted_merged_combined_global_yearly_2w_1980_top47k",
"reformatted_merged_combined_global_yearly_2w_1990_top47k",
"reformatted_merged_combined_global_yearly_2w_2000_top47k",
"reformatted_merged_combined_global_yearly_2w_2010_top47k",
"reformatted_merged_combined_global_yearly_2w_2019_top47k" ]

df_list = []

for directory in dirlist: 
    df = pd.read_csv(f"{directory}/SD.csv", header=None, dtype=str)
    df.index = df[0]
    df = df.drop(labels=[0], axis=1)
    colname = directory.split('_')[-2]
    df.columns = [ f"1958_to_{colname}" ]
    df_list.append(df)
    
total = pd.concat(df_list, axis=1)

total.to_csv("total_sd_combined.csv")
total.T.astype(np.float64).mean().to_csv("cluster_SD_means.csv", header=None)
total.astype(np.float64).mean().to_csv("yearly_SD_means.csv", header=None)

