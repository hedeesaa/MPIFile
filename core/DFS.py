import os
from uuid import uuid4
import math
import random
import glob


class DFS:

    CLUSTER_SIZE = 4096
        
    def __init__(self, dir, node_list):
        self.dir = dir
        self.node_list = node_list

    def create_dir(self):
        if os.path.isdir(self.dir):
            for f in os.listdir(self.dir):
                os.remove(os.path.join(self.dir, f))
        else:
            os.mkdir(self.dir)

    def get_dir(self):
        return self.dir
    
    def generate_record(self, filename):
        unique_id = str(uuid4())
        size = self.__get_size(filename)
        file_type=filename.split(".")[-1]
        return {"id":unique_id, "size":size, "type":file_type, "where":{}}
        
    def __get_size(self, filename):
        return os.stat(f"{self.dir}/{filename}").st_size
    
    def split_file(self, filename, uid):
        
        os.system(f'split -b {self.CLUSTER_SIZE} {self.dir}/{filename} {self.dir}/{uid}')
        arr = glob.glob(f"{self.dir}/{uid}*")
        for i in range(len(arr)):
            arr[i]= arr[i].split("/")[-1]

        return arr
   
    def __cluster_group(self, filename):
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

    def delete_file_master(self, filename):
        os.remove(f"{self.dir}/{filename}")
        
    def delete_chunks_master(self, filename, master_record):
        for _, chunks in master_record[filename]["where"].items():
            for chunk in chunks:
                os.remove(f"{self.dir}/{chunk}")

            
            

            


    

