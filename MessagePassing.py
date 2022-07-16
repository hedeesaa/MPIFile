from mpi4py import MPI

class MessagePassing:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
    
    def get_rank(self):
        return self.comm.Get_rank()
    
    def get_nodes_list(self):
        return self.comm.Get_size()-1
    
    def send(self, data, dest ):
        self.comm.send(data, dest=dest)
    
    def recv(self, src):
        data = self.comm.recv(source=src)
        return data