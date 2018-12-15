#===============================================================================
#!/usr/bin/python3
# Dan Zhao
# 28-11-2018
# find duplicates and save directory in csv file 
# next step remove duplicates and keep one copy
# Note! use this code at our own risk, I do not have responsibily 
# for any damange this code might cause 
#===============================================================================
import pandas as pd
import numpy as np
pics_meta_data = r'C:\Temp\photo_cleaner\all-list.csv'
list_to_keep = r'C:\Temp\photo_cleaner\keep-list.csv'
delete_list = r'C:\Temp\photo_cleaner\delete-list.csv'
df = pd.DataFrame(pd.read_csv(pics_meta_data, encoding='utf-8',sep='|'))
pic_cnt=len(df.index)
print(f'read in picture list and found {pic_cnt} pictures')
print('find unique filename/size with minimum directory length')
df_min = df.groupby(['CheckField'])['Length'].min()
#write result to csv to check content
df_min.to_csv(list_to_keep,encoding='utf-8',header=['LengthMin'],sep='|')
print('identify unique file names with minimum directory')
df_min_csv = pd.DataFrame(pd.read_csv(list_to_keep, encoding='utf-8',sep='|')) 
print('inner join and filter result to keep file name/size with shortest directory path') 
df_join = pd.merge(df,df_min_csv, how='left',left_on=['CheckField','Length'],right_on=['CheckField','LengthMin'])
df_join = df_join[(df_join.Filename != 'Thumbs.db')]
df_join = df_join[(df_join.LengthMin.isnull())]
print('save list of photo to be deleted into file delete-list.csv')
df_join.to_csv(delete_list, encoding='utf-8', sep='|')
print('delete-list.csv created')
#for checking duplicates
df_count = df_join.groupby(['Filename'])['CheckField'].count()
df_count[df_count>1].to_csv(r'C:\temp\photo_cleaner\duplicats.csv',encoding='utf-8',header=['count'],sep='|')
print('checking file complete')