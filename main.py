from core.server import MPIFile
from threading import Thread

from api import app

if __name__ == "__main__":
    mpif = MPIFile()
    if mpif.if_master():
        app.config['UPLOAD_FOLDER'] = mpif.get_dir()
        app.config['MPIF'] = mpif
        flask_thread = Thread(target=app.run)
        flask_thread.start()
    else:
        receive_thread = Thread(target=mpif.receive_file_from_master)
        receive_thread.start()

        send_thread = Thread(target=mpif.send_requested_file_to_master)
        send_thread.start()

        remove_thread = Thread(target=mpif.remove_file_on_nodes)
        remove_thread.start()