from api import app 
from flask import request,  send_file
import os
from werkzeug.utils import secure_filename
from threading import Thread

@app.route("/upload", methods=["POST"])
def upload():
    
    if request.method == 'POST':
        file = request.files['file']
        if file:
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            size = int(request.form["size"])
            filesize = int(os.stat(f"{app.config['UPLOAD_FOLDER']}/{filename}").st_size)

            if size == filesize:
                ## Upload to Others
                dfs = app.config["DFS"]
                mpi = app.config["MPI"]
                master_thread = Thread(target=send_files_to_nodes,args=(dfs,filename,mpi,))
                master_thread.start() 

                return "OK"
            else:
                return "The size is not correct, please try again"
    
        return "File hasnt been uploaded"
    
@app.route("/list", methods=["GET"])
def list_files():  
    dfs = app.config["DFS"]
    res=list(dfs.get_master_record().keys())
    res = ", ".join(res)
    return res

@app.route("/download/<path:filename>", methods=["GET"])
def download(filename):
    if check_if_file_exits(filename):
        ## gather files
        dfs = app.config["DFS"]
        mpi = app.config["MPI"]
        record=dfs.get_specific_record(filename)

        for node, chunks in record["where"].items():
            for chunk in chunks:
                mpi.send(chunk, dest=node,tag=2)
                dir_name = dfs.get_main_dir()
                with open(f"{dir_name}/{chunk}" , "+wb") as f:
                    data=mpi.recv(src=node,tag=2)
                    f.write(data)

        dir_name = dfs.get_main_dir()

        os.system(f'cat {dir_name}/{record["id"]}* > {dir_name}/{filename}')
        dfs.delete_chunks_master(filename)

        return send_file(f"../{app.config['UPLOAD_FOLDER']}/{filename}",as_attachment=True)
    else:
        return "This file doesnt exist"
    

@app.route("/remove", methods=["GET"])
def remove():
    filename=request.args.get("filename")
    if check_if_file_exits(filename):

        dfs = app.config["DFS"]
        mpi = app.config["MPI"]

        record=dfs.get_specific_record(filename)

        for node, chunks in record["where"].items():
            for chunk in chunks:
                mpi.send(chunk, dest=node,tag=3)

        try:
            dfs.delete_file_master(filename)
        except:
            pass

        del dfs.get_master_record()[filename]
        return "OK"
    return "The file doesnt exist"
    
def check_if_file_exits(filename):
    dfs = app.config["DFS"]
    if filename in dfs.get_master_record().keys():
        return True
    return False


def send_files_to_nodes(dfs_,filename_,mpi):
    record = dfs_.generate_record(filename_)
    ## Shred files
    record = dfs_.dedicate_nodes_to_chunkfiles(filename_,record,dfs_.split_file(filename_,record["id"]))
    dfs_.set_master_record(filename_,record)

    record=dfs_.get_specific_record(filename_)

    for node, chunks in record["where"].items():
        for chunk in chunks:
            mpi.send(chunk, dest=node,tag=1)
            dir_name = dfs_.get_main_dir()
            with open(f"{dir_name}/{chunk}" , "rb") as f:
                a=f.read()
                mpi.send(a, dest=node,tag=1)
                
    ## delete chuncks and files 
    dfs_.delete_file_master(filename_)
    dfs_.delete_chunks_master(filename_)


