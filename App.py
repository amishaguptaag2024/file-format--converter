import pandas as pd
import json
import glob
import os
import re
import sys


def get_schemas(schemas,table,sorting_key='column_position'):
    columns=schemas[table]
    sorted_column=sorted(columns,key=lambda col:col[sorting_key])
    return [col['column_name'] for col in sorted_column]

def readcsv(file,schemas,table_name):
    columns=get_schemas(schemas,table_name)
    csv_df=pd.read_csv(file,names=columns)
    return csv_df

def tojson(csv_df,tg_base_dir,table_name,file_name):
    os.makedirs(f'{tg_base_dir}/{table_name}',exist_ok=True)
    json_file_path=f'{tg_base_dir}/{table_name}/{file_name}'
    csv_df.to_json(json_file_path,orient='records',lines=True)

def tojson(csv_df,tg_base_dir,table_name,file_name):
    os.makedirs(f'{tg_base_dir}/{table_name}',exist_ok=True)
    json_file_path=f'{tg_base_dir}/{table_name}/{file_name}'
    csv_df.to_json(json_file_path,orient='records',lines=True)

def convert_csv_to_json(src_file_directory,ds_table,schemas,tg_base_dir):
    #tg_base_dir='data/retail_db_json'
    files=glob.glob(f'{src_file_directory}/{ds_table}/part-*')
    if len(files)==0:
        raise NameError(f'file path {src_file_directory}/{ds_table}/part-* does not exist')
        
    for file in files:
        file_name=re.split('[/\\\]',file)[-1]
        df=readcsv(file,schemas,ds_table)

        tojson(df,tg_base_dir,ds_table,file_name)

#DEFINE THE process_file using run time arguments
def process_file(ds_table=None):
    file_arg=sys.argv
    src_file_dir=file_arg[1] #'data/retail_db'
    tgt_base_dir=file_arg[2]
    schemas=json.load(open(f'{src_file_dir}/schemas.json'))
    if  not ds_table:
        ds_table=schemas.keys()
        
    for ds_name in ds_table:
        convert_csv_to_json(src_file_dir,ds_name,schemas,tgt_base_dir)

#DEFINE THE process_file as process_file_Env using environment variable
def process_file_Env(ds_table=None):
    src_file_dir=os.environ.get('SRC_BASE_DIR') #'data/retail_db'
    tgt_base_dir=os.environ.get('TGT_BASE_DIR') #'data/retail_db_json'
    schemas=json.load(open(f'{src_file_dir}/schemas.json'))
    if  not ds_table:
        ds_table=schemas.keys()
        
    for ds_name in ds_table:
        # if ds_name in schemas.keys():
        #     convert_csv_to_json(src_file_dir,ds_name,schemas,tgt_base_dir)
        # else:
        #     print(f'Table {ds_name} not exist')
        try:
            print(f'Processing table {ds_name}')
            convert_csv_to_json(src_file_dir,ds_name,schemas,tgt_base_dir)
        except NameError as ne:
            print(f'Error Processing {ds_name}: {ne}')
            pass

if __name__== '__main__':
    #process_file() #python App.py 'data/retail_db' 'data/retail_db_json' cmd to run
    ds_table=sys.argv
    if len(ds_table)==2:
        process_file_Env(json.loads(ds_table[1]))#python App.py
    else:
        process_file_Env()