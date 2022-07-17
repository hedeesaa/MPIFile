from MessagePassing import MessagePassing
from DFS import DFS
import time
from threading import Thread
from flask import Flask, request
from werkzeug.utils import secure_filename
import sys
import json



MASTER = 0
mp = MessagePassing()
rank = mp.get_rank()

all_master_record = ""


def master_func(fm):
    time.sleep(10)
    FileName = "comp6231.2022s.a03.assignment.iii.pdf"
    master_record = fm.generate_record(FileName)
    global all_master_record
    all_master_record = master_record
    print(master_record,file=sys.stderr)
    print("after calling master record")

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
        
def api_server(fm):
    app = Flask(__name__)

    
    @app.route('/upload', methods=['POST'])
    def update():
        app.config['UPLOAD_FOLDER'] = "./here/"
        if request.method == 'POST':
            file = request.files['file']
            result = json.dumps(request.form)
            print(result ,file=sys.stderr)
            import os
            if file:
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return "OK"
    
    @app.route('/list', methods=['GET'])
    def list_files():
        return all_master_record
    
    

    app.run()   

if rank == MASTER:
    fm = DFS(directory_name="master_files",node_list=mp.get_nodes_list())

    api_thread = Thread(target=api_server, args=(fm,))
    api_thread.start()

else:
    print(f"My rank is {rank}")
    node_thread = Thread(target=node_func)
    node_thread.start()