from flask import Flask, render_template
from flask_socketio import SocketIO
from dashboard.metrics_store import MetricsStore
from common.config import DASHBOARD_PORT
import threading, time
import json, socket, threading

METRICS_UDP_PORT = 9004

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
    threading.Thread(target=_udp_metrics_listener, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=DASHBOARD_PORT)

def _udp_metrics_listener():
    """Receives metric events forwarded from Node B over UDP."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", METRICS_UDP_PORT))
    print(f"[Dashboard] Listening for remote metrics on UDP {METRICS_UDP_PORT}")
    while True:
        data, addr = sock.recvfrom(4096)
        try:
            event = json.loads(data.decode())
            _store.record(event)
            socketio.emit("m", _store.snapshot())
        except Exception as e:
            print(f"[Dashboard] Bad metric packet: {e}")


@app.route("/")
def index():
    return render_template("index.html")
