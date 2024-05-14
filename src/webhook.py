from flask import Flask, request
import datetime as dt

app = Flask(__name__)

@app.route('/', methods=['GET'])
def webhook():
    if request.method == 'GET':
        print("Data received from Webhook is: ", request.data)
        return f"Webhook received! {dt.datetime.now()}"

app.run(host='0.0.0.0', port=8000)