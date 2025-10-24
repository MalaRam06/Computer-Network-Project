from flask import Flask, request
import os, socket

app = Flask(__name__)
INSTANCE = f"{socket.gethostname()}:{os.environ.get('PORT','8001')}"
counter = 0

@app.route("/")
def index():
    global counter
    counter += 1
    return {
        "message": "Hello from backend",
        "instance": INSTANCE,
        "request_count_here": counter,
        "client_ip": request.headers.get('x-forwarded-for', request.remote_addr)
    }

@app.route("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8001"))
    app.run(host="0.0.0.0", port=port)
