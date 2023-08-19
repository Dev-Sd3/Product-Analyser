from flask import Flask, render_template, request, redirect, url_for, session
from flask_session import Session
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from Frontend.quickData import getDatabase, getItems
from Backend.interface import interface
import pandas as pd
import os
import secrets
import threading
import warnings

warnings.filterwarnings("ignore")

template_dir = os.path.abspath('Frontend/templates')
static_dir = 'Frontend/static'


app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
app.config.from_object(__name__)
app.config['SESSION_TYPE'] = 'filesystem'
app.secret_key = secrets.token_hex(16)
Session(app)

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["100 per day"],
    storage_uri="memory://"
)


lock = threading.Lock()
historyDf, analysisDf, formatted_entries, historyEntries = getDatabase()


@app.route('/', methods=['GET', 'POST'])
@app.route('/home', methods=['GET', 'POST'])
@limiter.limit("10 per minute")
def index():
    if not "running" in session:
        session["running"] = False

    historyDf, analysisDf, formatted_entries, historyEntries = getDatabase()

    if request.method == 'POST':
        if not session["running"]:
            with lock:
                session["running"] = True
                item = request.form.get('item')
                action = request.form.get('action')
                selected_items = request.form.getlist('selected_items')

                if action == 'refresh':
                    interface(historyEntries, False)

                if action == 'append':
                    items = getItems(item)
                    items = [item for item in items if item != '']
                    interface(items, False,True)

                if action == 'custom':
                    list = getItems(item)
                    list = [item for item in list if item != '']
                    list += selected_items
                    interface(list, True)

                session["running"] = False

        else:
            print("Already Running")

    return render_template('home.html', history=formatted_entries)


@app.route('/history')
@limiter.exempt
def history():
    historyDf, analysisDf, formatted_entries, historyEntries = getDatabase()
    return render_template('tables.html',  column_names=historyDf.columns.values, row_data=list(historyDf.values.tolist()), zip=zip)


@app.route('/analysis')
@limiter.exempt
def analysis():
    historyDf, analysisDf, formatted_entries, historyEntries = getDatabase()
    return render_template('tables.html',  column_names=analysisDf.columns.values, row_data=list(analysisDf.values.tolist()), zip=zip)


if __name__ == '__main__':
    app.run(debug=True)
