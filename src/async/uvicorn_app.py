from asgiref.wsgi import WsgiToAsgi
from flask import Flask
from flask_cors import CORS


def create_app():
    app = Flask(__name__)
    CORS(app)
    return app

app = create_app()

@app.route("/uv")
def home_gu():
    return "uv"

@app.route("/uv/<int:id>")
def home_du_id(id):
    return f"uv {id}"

asgi_app = WsgiToAsgi(app)

if __name__ == '__main__':        
    import uvicorn
    uvicorn.run(asgi_app)#, host=APP_HOST, port=APP_PORT)
