from api import app 
from flask import request,  send_file
import os
from werkzeug.utils import secure_filename
from threading import Thread


def save_file_on_master(file):
    filename = secure_filename(file.filename)
    if not app.config['MPIF'].check_if_file_exits(filename):
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        size = int(request.form["size"])
        filesize = int(os.stat(f"{app.config['UPLOAD_FOLDER']}/{filename}").st_size)
        if size == filesize:
            master_thread = Thread(target=app.config['MPIF'].send_files_to_nodes,args=(filename,))
            master_thread.start() 
            return "OK"
        else:
            return "ERROR: SIZE IS NOT CORRECT"
    return f"ERROR: File {filename} EXISTS ON THE SERVER"


@app.route("/upload",methods=["POST"])
def upload():
    if request.method == "POST":
        file = request.files['file']
        if file:
            return save_file_on_master(file)
    
        return "ERROR: UPLOAD NOT SUCCESSFUL"


@app.route("/list", methods=["GET"])
def list_files(): 
    res=list(app.config["MPIF"].get_master_record().keys())
    return ", ".join(res)


@app.route("/download/<path:filename>", methods=["GET"])
def download(filename):
    if app.config['MPIF'].check_if_file_exits(filename):
        ## gather files
        app.config['MPIF'].request_to_receive_file_from_nodes(filename)

        return send_file(f"../{app.config['UPLOAD_FOLDER']}/{filename}",as_attachment=True)
    return f'File "{filename}" Does not Exist'
    

@app.route("/remove", methods=["GET"])
def remove():
    filename=request.args.get("filename")
    if app.config['MPIF'].check_if_file_exits(filename):
        app.config['MPIF'].ask_remove_file_from_master(filename)
        return "OK"
    return f'File "{filename}" Does not Exist'
 