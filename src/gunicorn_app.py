from flask import Flask

app = Flask(__name__)

@app.route("/gu")
def home_gu():
    return "gu"

@app.route("/gu/<int:id>")
def home_du_id(id):
    return f"gu {id}"

if __name__ == '__main__':
    app.run()