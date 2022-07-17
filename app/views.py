from app import app 

@app.route("/list")
def index():
    return "Hello"