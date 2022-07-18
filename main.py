from MessagePassing.MessagePassing import MessagePassing
from api import app
from DFS import DFS
from threading import Thread

def save_file(dfs_node,mp):
    while True:
        file = mp.recv(src=0,tag=1)
        dir_name = dfs_node.get_main_dir()
        with open(f"{dir_name}/{file}" ,"+wb") as f:
            data = mp.recv(src=0, tag=1)
            f.write(data)


def send_file(dfs_node,mp):
    while True:
        file = mp.recv(src=0,tag=2)
        dir_name = dfs_node.get_main_dir()
        with open(f"{dir_name}/{file}" ,"rb") as f:
            data=f.read()
            mp.send(data,dest=0, tag=2)
      
def server():

    MASTER = 0
    mp = MessagePassing()
    rank = mp.get_rank()

    if rank == MASTER:
        print(f"Master-{rank} is Ready...")

        try:

            dfs = DFS(directory_name="master_files",
                        node_list=mp.get_nodes_list())
            dfs.create_dir()

            app.config['UPLOAD_FOLDER']="master_files"

            app.config['DFS'] = dfs

            app.config['MPI'] = mp

            app.run()
        except:
            print("Couldnt Create Directory")
            exit

    else:
        print(f"Node-{rank} is Ready...")
        dfs_node = DFS(directory_name=f"{rank}_files")
        dfs_node.create_dir()

        save_thread = Thread(target=save_file, args=(dfs_node,mp))
        save_thread.start()

        send_thread = Thread(target=send_file, args=(dfs_node,mp))
        send_thread.start()


if __name__ == "__main__":
    server()