from flask import Flask
from flask import render_template
from flask import jsonify
from web_utils import *

app = Flask(__name__)


@app.route("/")
def hello_word():
    return render_template("main.html")


@app.route("/timing")
def get_time():
    return get_times()


@app.route("/c1")
def get_c1_data():
    data = get_c1_data()
    dic = {'confirm': data[0],
           'suspect': data[1],
           'heal': data[2],
           'dead': data[3]}
    return jsonify(dic)


if __name__ == '__main__':
    app.run()
