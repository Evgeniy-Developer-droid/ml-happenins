from flask import Flask, jsonify, request
from sshtunnel import SSHTunnelForwarder
import pymysql
import pandas as pd
from pathlib import Path
from ML import ML
from loguru import logger

logger.add('logs/logs.log', level='DEBUG', rotation="500 MB")

path = Path(__file__).parent.absolute()

try:
    tunnel = SSHTunnelForwarder(('142.93.144.14', 22), ssh_password='uYnTUQ4dWLtJaPvK', ssh_username='root',
                                ssh_pkey=str(path)+'/id_rsa',
                                remote_bind_address=('127.0.0.1', 3306))
    tunnel.start()
    conn = pymysql.connect(host='127.0.0.1', user='root', database='happenin', passwd='Eg482zetkxNNZDSP', port=tunnel.local_bind_port)
except pymysql.Error as e:
    logger.warning(e)



app = Flask(__name__)


@app.route('/')
def index():
    return "<h1>API for happenin</h1>"

@app.route('/api/get-events-by-user')
def get_events_by_user():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify([])
    try:
        last_rows_user = pd.read_sql_query("SELECT * FROM users_actions WHERE user_id = "+user_id+" AND id>0 ORDER BY id DESC LIMIT 5", conn)
        events_all = pd.read_sql_query("SELECT * FROM event_data WHERE (event_type IN ('DAILY', 'WEEKLY', 'MON_FRI', 'SAT_SUN') OR (event_type = 'ONE_TIME' AND start_time >= NOW())) AND is_canceled = 0 AND deleted = 0", conn)
    except pd.io.sql.DatabaseError as e:
        logger.warning(e)
        return jsonify({'error': "sql query error"})
    model = ML(last_rows_user, events_all)
    result = model.get_events()
    # return events_all.to_json(orient="records")
    return jsonify({"events_ids": result})