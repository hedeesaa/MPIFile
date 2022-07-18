import os
from uuid import uuid4
import math
import random
import glob


class DFS:

    CLUSTER_SIZE = 4096

    def __init__(self,directory_name,node_list={}):
        self.main_dir = directory_name
        self.node_list = node_list
        self.master_record = {}
        
    def create_dir(self):
        if os.path.isdir(self.main_dir):
            for f in os.listdir(self.main_dir):
                os.remove(os.path.join(self.main_dir, f))
        else:
            os.mkdir(self.main_dir)

    
    def generate_record(self, filename):
        unique_id = str(uuid4())
        size = self.__get_size(filename)
        file_type=filename.split(".")[-1]
        return {"id":unique_id, "size":size, "type":file_type, "where":{}}
        

     
    def __get_size(self,filename):
        return os.stat(f"{self.main_dir}/{filename}").st_size
    
    def split_file(self,filename,uid):
        
        os.system(f'split -b {self.CLUSTER_SIZE} {self.main_dir}/{filename} {self.main_dir}/{uid}')
        arr = glob.glob(f"{self.main_dir}/{uid}*")
        for i in range(len(arr)):
            arr[i]= arr[i].split("/")[-1]

        return arr
   
    def __cluster_group(self,filename):
        return math.ceil(self.__get_size(filename)/DFS.CLUSTER_SIZE)

    def dedicate_nodes_to_chunkfiles(self,filename,record,splited_files):
        ## get random nodes 
        for i in range(self.__cluster_group(filename)):
            num=random.randint(1,self.node_list)
            if num in record["where"]:
                record["where"][num].append(splited_files[i])
            else:
                record["where"][num]= [splited_files[i]]
        return record
    
    def set_master_record(self,filename,mr):
        self.master_record[filename] = mr

    def get_specific_record(self,filename):
        return self.master_record[filename]

    def get_master_record(self):
        return self.master_record

    def get_main_dir(self):
        return self.main_dir

    def delete_file_master(self,filename):
        os.remove(f"{self.main_dir}/{filename}")
        
    def delete_chunks_master(self,filename):
        for _, chunks in self.master_record[filename]["where"].items():
            for chunk in chunks:
                os.remove(f"{self.main_dir}/{chunk}")


            
            

            


    

