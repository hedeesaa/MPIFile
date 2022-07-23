from mpi4py import MPI

class MessagePassing:
    
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
    
    def get_nodes_list(self):
        return self.comm.Get_size()-1

    def send(self, data, dest, tag):
        self.comm.send(data, dest=dest,tag= tag)

    def recv(self, src, tag):
        data = self.comm.recv(source=src,tag=tag)
        return data

    def send_file(self,chunk,node,directory):
        self.send(chunk, dest=node,tag=1)
        with open(f"{directory}/{chunk}" , "rb") as f:
            self.send(f.read(), dest=node,tag=1)

    def receive_file(self,directory):
        file = self.recv(src=0,tag=1)
        with open(f"{directory}/{file}" ,"+wb") as f:
            f.write(self.recv(src=0, tag=1))

    def request_to_receive_file(self,chunk,node, directory):
        self.send(chunk, dest=node,tag=2)
        with open(f"{directory}/{chunk}" , "+wb") as f:
            f.write(self.recv(src=node,tag=2))
    
    def send_back_requested_file(self, directory):
        file = self.recv(src=0,tag=2)
        with open(f"{directory}/{file}" ,"rb") as f:
            self.send(f.read(),dest=0, tag=2)


    