#===============================================================================
#!/usr/bin/python3
# Dan Zhao
# 22-11-2018
# remove duplicates and keep one copy
# Note! use this code at our own risk, I do not have responsibily 
# for any damange this code might cause 
#===============================================================================
import os
import pandas as pd
delete_list = r'C:\Temp\photo_cleaner\delete-list.csv'
delete_dirs = r'C:\Temp\photo_cleaner\delete-dirs.csv'
df = pd.DataFrame(pd.read_csv(delete_list,encoding='utf-8',sep='|')) 
print('loaded '+delete_list)
df_dir = df[['Directory']]
df_dir.to_csv(delete_dirs,encoding='utf-8',header=['Directory'],sep='|')
print('prepared directories to delete in file '+ delete_dirs)
with open(delete_dirs, "r", encoding="utf-8") as file:
    print('reading '+delete_dirs)
    a=file.readline()
    #print('skip header')
    n=0
    print('start looping through the directories') 
    for line in file:
        p=line[line.index('|')+1:-1]
        os.remove(p)
        print('removing file count '+str(n))
        n=n+1
print(str(n)+' files removed')