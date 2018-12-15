#===============================================================================
#!/usr/bin/python3
# Dan Zhao
# 22-11-2018
# copy picture to yyyymm directory 
# keeping the meta data -> use shutil.copy2
# Note! use this code at our own risk, I do not have responsibily 
# for any damange this code might cause 
#===============================================================================
from shutil import copy2
import os
import pandas as pd
pics_meta_data = r'C:\Temp\photo_cleaner\all-list.csv'
copy_dir = r'C:\Temp\photo_cleaner\copy-dir.csv'
df = pd.DataFrame(pd.read_csv(pics_meta_data,encoding='utf-8',sep='|')) 
print('loaded '+pics_meta_data)
df_dir = df[['Directory','yyyymm_dir']]
df_dir.to_csv(copy_dir,encoding='utf-8',header=['Directory','yyyymm_dir'],sep='|')
print('prepared directories to copy in file '+ copy_dir)


with open(copy_dir, "r", encoding="utf-8") as file:
    print('reading '+copy_dir)
    a=file.readline()
    #print('skip header')
    n=0
    str_dot="."
    print('start looping through the directories') 
    for line in file:
        if line.find('Thumbs.db') == -1:
            first_pipe_position = line.find('|')
            src=line[first_pipe_position+1 : line.find('|', first_pipe_position +5) ]
            dsc=line[line.find('|',6)+1 : -1 ]
            if not os.path.exists(line[line.find('Photo_Time_Line')-3 : line.find(r'Time_Line')+17]):
                os.mkdir(line[line.find('Photo_Time_Line')-3 : line.find(r'Time_Line')+17])
                print('making directory '+line[line.find('Photo_Time_Line')-3 : line.find(r'Time_Line')+17])
                #str_dot="."
            #print(line.encode('utf-8'))
            #print(str(src.encode('utf-8'))+' & '+ str(dsc.encode('utf-8')))
            #print('copy in directory '+line[line.find('Photo_Time_Line')-3 : line.find(r'Time_Line')+17])
            copy2(src,dsc)
            #print(str_dot)
            #print('copying file count '+str(n))
            n=n+1
            #str_dot=str_dot+"."
print(str(n)+' files copied')