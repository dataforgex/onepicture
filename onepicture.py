#===============================================================================
#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Dan Zhao
# 12-07-2020
#===============================================================================
import os
import datetime
import pandas as pd

def rowID_gen(n):
    for i in range(1,n,1):
        yield i
rowID=rowID_gen(1000000)

def make_df_from_file_meta_data(picture_dir,time_line_dir,redundent_file_dir):
    '''
    scan directory for files and return dataframe
    '''
    csvlist=[]
    for root, dirs, filenames in os.walk(picture_dir):
        # filenames is a list object therefore loop is required below
        for filename in filenames:
            rID = next(rowID)
            Filename = filename
            Full_path = os.path.join(root,filename)
            ModifiedTime = str(datetime.datetime.fromtimestamp(os.stat(Full_path).st_mtime))
            yyyymm_dir = time_line_dir+'\\'+ModifiedTime[:7]+'\\'+Filename
            delete_dir = redundent_file_dir+'\\'+str(rID)+Filename
            Length = len(Full_path)
            SizeKB = str(os.stat(Full_path).st_size/1024)
            CheckField = Filename + SizeKB
            csvlist.append([rID, Filename, Full_path, yyyymm_dir,delete_dir,\
                            Length, SizeKB, ModifiedTime, CheckField]
                          ) 
    df=pd.DataFrame(csvlist)
    mapping={df.columns[0]:'rID'
            ,df.columns[1]:'Filename'
            ,df.columns[2]:'Full_path'
            ,df.columns[3]:'yyyymm_dir'
            ,df.columns[4]:'delete_dir'
            ,df.columns[5]:'Length'
            ,df.columns[6]:'SizeKB'
            ,df.columns[7]:'ModifiedTime'
            ,df.columns[8]:'CheckField'
            }
    df.rename(columns=mapping,inplace=True)
    cnt=len(df.index)
    print(f'--***--Program found in total {cnt} pictures in directory '+my_picture_dir)
    return df

def create_csv_from_df(df,csv_dir):
    '''
    create csv file from dataframe
    '''
    with open(csv_dir, 'w', encoding='utf-8') as outputfile:
        df.to_csv(outputfile,sep="|"
                 ,index=False
                 ,encoding="utf-8"
                 ,line_terminator='\n'
                 ,header=['rID'
                         ,'Filename'
                         ,'Full_path'
                         ,'yyyymm_dir'
                         ,'delete_dir'
                         ,'Length'
                         ,'SizeKB'
                         ,'ModifiedTime'
                         ,'CheckField'
                         ]
                 )
    print('--***--Program is saving file list to '+csv_dir)

def find_unique_files(df,csv_dir):
    '''
    find unique files and log result to csv file
    return dataframe
    '''
    print('--***--Program finding unique files based on name and size')
    df1 = df[['CheckField', 'Length']]
    df_unique = df1.groupby(['CheckField'], as_index=False, sort=False)['Length'].min()
    df_unique.to_csv(csv_dir,encoding='utf-8',header=['CheckField','LengthMin'],sep='|')
    print(f'--***--Program found in total {len(df_unique.index)} unique files')
    print('--***--Program is saving unique file list to: '+csv_dir)
    df_unique.columns = ['CheckFieldUnique','LengthMin']
    return df_unique

def moving_duplicated_files(df_all,df_unique,csv_dir):
    '''
    find and move duplicated files to deleteme directory and log result to csv file
    return dataframe
    '''
    print('--***--Program is idendtifing redundent files.') 
    df_join = pd.merge(df,df_unique, how='left',left_on=['CheckField','Length'],right_on=['CheckFieldUnique','LengthMin'])
    df_to_delete = df_join[df_join.Filename != 'Thumbs.db']
    df_to_delete = df_to_delete[df_to_delete['LengthMin'].isna()]
    file_count = df_to_delete[['rID']].count()
    print(f'--***--Program found {file_count.values} redundent files' )
    df_to_delete.to_csv(csv_dir, encoding='utf-8', sep='|',index=False)
    n=1
    for ind in df_to_delete.index:
        print('--***--Program is moving {} to {}'.format(df_to_delete['Full_path'][ind],df_to_delete['delete_dir'][ind]))
        os.rename(df_to_delete['Full_path'][ind], df_to_delete['delete_dir'][ind])
        print('--***--moved files count '+str(n))
        n=n+1


if __name__=="__main__":
    print('--***--Program onepicture starting...')
    #pd.set_option('display.expand_frame_repr', False)
    scanned_files_log_dir = r'D:\onepicture_logs\all-list.csv'
    my_picture_dir = r'D:\ALL_PICTURES'
    my_photo_time_line_dir = r'D:\Photo_Time_Line'
    redundent_file_dir = r'D:\onepicture_deleteme'
    files_to_keep_csv_dir = r'D:\onepicture_logs\keep-list.csv'
    files_to_delete_csv_dir = r'D:\onepicture_logs\delete-list.csv'
    
    df = make_df_from_file_meta_data(my_picture_dir, my_photo_time_line_dir, redundent_file_dir)
    create_csv_from_df(df, scanned_files_log_dir)
    df_unique = find_unique_files(df,files_to_keep_csv_dir)
    moving_duplicated_files(df,df_unique,files_to_delete_csv_dir)
    print('--***--Program onepicture ended')    

