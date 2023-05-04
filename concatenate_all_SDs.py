import os
import pandas as pd
import numpy as np
import sys 
from tqdm import tqdm

base_path = sys.argv[1]
dirlist = list(filter(lambda x: "top47k" in x,  os.listdir(base_path)))
# [
#"reformatted_merged_combined_global_yearly_2w_1960_top47k",
#"reformatted_merged_combined_global_yearly_2w_1970_top47k",
#"reformatted_merged_combined_global_yearly_2w_1980_top47k",
#"reformatted_merged_combined_global_yearly_2w_1990_top47k",
#"reformatted_merged_combined_global_yearly_2w_2000_top47k",
#"reformatted_merged_combined_global_yearly_2w_2010_top47k",
#"reformatted_merged_combined_global_yearly_2w_2019_top47k" ]



df_list = []

for directory in tqdm(dirlist): 
    if "58" in directory: 
        continue
    df = pd.read_csv(f"{base_path}/{directory}/SD.csv", header=None, dtype=str)
    df.index = df[0]
    df = df.drop(labels=[0], axis=1)
    colname = directory.split('_')[-2]
    df.columns = [ f"1958_to_{colname}" ]
    df_list.append(df)
    
print("Concatenating") 
total = pd.concat(df_list, axis=1)

print("Saving to file: ")
total.to_csv(f"{base_path}/total_sd_combined.csv")
total.T.astype(np.float64).mean().to_csv(f"{base_path}/cluster_SD_means.csv", header=None)
total.astype(np.float64).mean().to_csv(f"{base_path}/yearly_SD_means.csv", header=None)

