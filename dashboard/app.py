from flask import Flask, render_template
from flask_socketio import SocketIO
from dashboard.metrics_store import MetricsStore
from common.config import DASHBOARD_PORT
import threading, time

app      = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")
_store   = MetricsStore()

def metrics_cb(event: dict):
    _store.record(event)
    socketio.emit("m", _store.snapshot())

def start_dashboard():
    def push():
        while True:
            socketio.emit("m", _store.snapshot())
            time.sleep(1)
    threading.Thread(target=push, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=DASHBOARD_PORT)

@app.route("/")
def index():
    return render_template("index.html")
