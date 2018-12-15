#===============================================================================
#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Dan Zhao
# 22-11-2018
# OS windows 8
# python 3.6.7
# print file information from a given directory 
# works with sub folders
# save file check result into a csv file on a defined directory
# Note! use this code at our own risk, I do not have responsibily 
# for any damange this code might cause 
#===============================================================================
import os
import datetime
import pandas as pd
def rowID_gen(n):
    for i in range(1,n,1):
        yield i
rowID=rowID_gen(1000000)
csvlist=[]
pics_meta_data = r'C:\Temp\photo_cleaner\all-list.csv'
picture_dir = r'E:\ALL_PICTURES'
time_line_dir = r'E:\Photo_Time_Line'
for root, dirs, filenames in os.walk(picture_dir):
    # filenames is a list object therefore loop is required below
    for filename in filenames:
        rID = next(rowID)
        Filename = filename
        Directory = os.path.join(root,filename)
        yyyymm_dir = time_line_dir+'\\'+str(datetime.datetime.fromtimestamp(os.stat(os.path.join(root,filename)).st_mtime))[:7]+'\\'+Filename
        Length = len(Directory)
        SizeKB = str(os.stat(os.path.join(root,filename)).st_size/1024)
        ModifiedTime = str(datetime.datetime.fromtimestamp(os.stat(os.path.join(root,filename)).st_mtime))
        CheckField = Filename + SizeKB #+ ModifiedTime
        csvlist.append([rID, Filename, Directory, yyyymm_dir, Length, SizeKB, ModifiedTime, CheckField]) 
df=pd.DataFrame(csvlist)
with open(pics_meta_data, 'w', encoding='utf-8') as outputfile:
    df.to_csv(outputfile ,index=False, encoding='utf-8', sep='|' \
    ,header=['rID','Filename','Directory','yyyymm_dir', 'Length','SizeKB','ModifiedTime','CheckField'])
print('csv file prepared in '+ pics_meta_data )