import os
from core.MessagePassing import MessagePassing
from core.DFS import DFS

class MPIFile(MessagePassing, DFS):

    MASTER = 0

    def __init__(self):
        MessagePassing.__init__(self)
        self.dir = str(self.rank)+"_files"
        DFS.__init__(self, self.dir, MessagePassing.get_nodes_list(self))
        DFS.create_dir(self)
        self.master_record = {}

    def if_master(self):
        if self.rank == MPIFile.MASTER:
            return True
        return False
    
    def set_master_record(self,filename,record):
        self.master_record[filename] = record

    def get_master_record(self):
        return self.master_record
    
    def get_specific_record(self,filename):
        return self.master_record[filename]

    def send_files_to_nodes(self, filename):
        record = DFS.generate_record(self,filename)
        ## Shured files
        record = DFS.dedicate_nodes_to_chunkfiles(self,filename,record,DFS.split_file(self,filename,record["id"]))

        self.set_master_record(filename,record)

        for node, chunks in record["where"].items():
            for chunk in chunks:
                MessagePassing.send_file(self,chunk,node,DFS.get_dir(self))
                    
        # ## delete chuncks and files 
        DFS.delete_file_master(self,filename)
        DFS.delete_chunks_master(self,filename,self.get_master_record())


    def receive_file_from_master(self):
        while True:
            MessagePassing.receive_file(self,DFS.get_dir(self))

    def request_to_receive_file_from_nodes(self, filename):
        record=self.get_specific_record(filename)
        dir_ = DFS.get_dir(self)
        for node, chunks in record["where"].items():
            for chunk in chunks:
                MessagePassing.request_to_receive_file(self,chunk, node, dir_)
        os.system(f'cat {dir_}/{record["id"]}* > {dir_}/{filename}')
        DFS.delete_chunks_master(self,filename,self.get_master_record())

    def send_requested_file_to_master(self):
        while True:
            MessagePassing.send_back_requested_file(self,DFS.get_dir(self))
    
    def ask_remove_file_from_master(self, filename):
        record=self.get_specific_record(filename)
        for node, chunks in record["where"].items():
            for chunk in chunks:
                MessagePassing.send(self, chunk, dest=node,tag=3)

        try:
            DFS.delete_file_master(self,filename)
        except:
            pass

        del self.get_master_record()[filename]

    def remove_file_on_nodes(self):
        while True:
            file = MessagePassing.recv(self,src=0,tag=3)
            os.remove(f"{self.get_dir()}/{file}")

    def check_if_file_exits(self, filename):
        if filename in self.master_record.keys():
            return True
        return False
