from MessagePassing import MessagePassing
from DFS import DFS
import time
from threading import Thread

MASTER = 0
mp = MessagePassing()
rank = mp.get_rank()



def master_func():
    fm = DFS(directory_name="master_files",node_list=mp.get_nodes_list())
    time.sleep(10)
    FileName = "comp6231.2022s.a03.assignment.iii.pdf"
    master_record = fm.generate_record(FileName)
    print(master_record)

    for node, chunks in master_record[FileName]["where"].items():
        for chunk in chunks:

            mp.send(chunk, dest=node)
            with open(f"master_files/{chunk}" , "rb") as f:
                a=f.read()
                mp.send(a, dest=node)

    ## delete chuncks and files 
    fm.delete_file_master(FileName)

def node_func():
    dir_name = f"{rank}_files"
    fm = DFS(directory_name=dir_name,node_list=0)
    while True:
        file = mp.recv(src=0)
        with open(f"{dir_name}/{file}" ,"+wb") as f:
                data = mp.recv(src=0)
                f.write(data)
        


if rank == MASTER:
    print("You are Master")
    

    ###### API
    ## Waits for API call
    ## saves the API file
    ## Check if the File and size is ok
    ######
    
    master_thread = Thread(target=master_func)
    master_thread.start()

else:
    print(f"My rank is {rank}")
    node_thread = Thread(target=node_func)
    node_thread.start()
