import glob
import os
import json
import pandas as pd
import re
import sys



# To get the column names from the files
def get_column_names(schemas,des_name,sorting_key='column_position'):
  column_details=schemas[des_name]
  columns=sorted(column_details,key=lambda col:col[sorting_key])
  return [col['column_name'] for  col in columns] 


# reads the  csv file
def read_csv(file,schemas):
  file_path_list=re.split('[/\\\]',file) #splits the string  by / or \ to give a list of strings
  des_name=file_path_list[-2]
  # file_name=file_path_list[-1]
  columns=get_column_names(schemas,des_name)# get the column names and column list
  df=pd.read_csv(file,names=columns) # csv files that convert into dataframe format 
  return df

def to_json(df,tgt_base_dir,des_name,file_name):
    json_file_path=f'{tgt_base_dir}/{des_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{des_name}',exist_ok=True)
    df.to_json(
        json_file_path,
        orient='records',
        lines=True
      )
    
def file_converter(src_base_dir,tgt_base_dir,des_name):  
  schemas=json.load(open(f'{src_base_dir}/schemas.json'))
  files=glob.glob(f'{src_base_dir}/{des_name}/part-*')
  #Expection handling
  if len(files)==0:
    raise NameError(f'No files found for{des_name}')

  for file in files:
    
    df=read_csv(file,schemas)
    file_name=re.split('[/\\\]',file)[-1]
    to_json(df,tgt_base_dir,des_name,file_name)
    

def process_files(des_names=None):
  src_base_dir=os.environ.get('SRC_BASE_DIR')
  tgt_base_dir=os.environ.get('TGT_BASE_DIR')
  schemas=json.load(open(f'{src_base_dir}/schemas.json'))
  if not des_names:
    des_names=schemas.keys()
  for des_name in des_names:
    try:
      print(f'processing {des_name}')
      file_converter(src_base_dir,tgt_base_dir,des_name)
    except NameError as ne:
      print(ne)
      print(f'Error processing {des_name}')
      pass



#happy path
if __name__ == '__main__':
  if len(sys.argv)==2:
    des_names = json.loads(sys.argv[1])
    process_files(des_names)
  else:
    process_files()


