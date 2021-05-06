from flask import Flask, jsonify, request
from sshtunnel import SSHTunnelForwarder
import pymysql
import pandas as pd
from pathlib import Path

path = Path(__file__).parent.absolute()

tunnel = SSHTunnelForwarder(('142.93.144.14', 22), ssh_password='uYnTUQ4dWLtJaPvK', ssh_username='root',
      ssh_pkey=str(path)+'/id_rsa',
      remote_bind_address=('127.0.0.1', 3306))
tunnel.start()
conn = pymysql.connect(host='127.0.0.1', user='root', database='happenin', passwd='Eg482zetkxNNZDSP', port=tunnel.local_bind_port)




app = Flask(__name__)


@app.route('/')
def index():
  return "<h1>API for happenin</h1>"

@app.route('/api/get-events-by-user')
def get_events_by_user():
  user_id = request.args.get('user_id')
  if not user_id:
    return jsonify([])
  data = pd.read_sql_query("SELECT * FROM users_actions WHERE user_id = "+user_id, conn)
  return data.to_json()
