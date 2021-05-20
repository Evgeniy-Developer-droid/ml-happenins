from flask import Flask, jsonify, request
from sshtunnel import SSHTunnelForwarder
import pymysql
import pandas as pd
from pathlib import Path
from ML import ML
from loguru import logger
import datetime

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
    range = request.args.get('range')
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    if not user_id:
        return jsonify([])
    try:
        last_rows_user = pd.read_sql_query("SELECT * FROM users_actions WHERE user_id = '"+user_id+"' AND id>0 AND action != 0 ORDER BY id DESC LIMIT 5", conn)
        x = datetime.datetime.now()
        query = """SELECT * FROM event_data WHERE (((DATE_FORMAT(NOW(), '%H:%i:%s')
                              BETWEEN DATE_SUB(STR_TO_DATE(start_time, '%H:%i:%s'), INTERVAL 1 HOUR) and STR_TO_DATE(end_time, '%H:%i:%s'))
                              and """+x.strftime("%A").lower()+"""= 1) OR ((DATE_FORMAT(NOW(), '%H:%i:%s') > DATE_SUB(STR_TO_DATE(start_time, '%H:%i:%s'),
                              INTERVAL 1 HOUR) and DATE_FORMAT(NOW(), '%H:%i:%s') < DATE_ADD(STR_TO_DATE(end_time, '%H:%i:%s'), INTERVAL 24 HOUR))
                              and """+x.strftime("%A").lower()+""" = 1) OR ((DATE_FORMAT(NOW(), '%H:%i:%s') < DATE_SUB(STR_TO_DATE(start_time, '%H:%i:%s'),
                              INTERVAL 1 HOUR) and DATE_FORMAT(NOW(), '%H:%i:%s') < STR_TO_DATE(end_time, '%H:%i:%s')
                              and STR_TO_DATE(start_time, '%H:%i:%s') > STR_TO_DATE(end_time, '%H:%i:%s')) and """+x.strftime("%A").lower()+""" = 1)
                              OR (NOW() BETWEEN DATE_SUB(STR_TO_DATE(start_time, '%Y-%m-%d %H:%i:%s'), INTERVAL 1 HOUR)
                              and STR_TO_DATE(end_time, '%Y-%m-%d %H:%i:%s')) and event_type = 'ONE_TIME') AND is_canceled = 0 AND deleted = 0 AND (
                                6371 * acos(
                                  cos(
                                    radians("""+lat+""")
                                  ) * cos(
                                    radians(latitude)
                                  ) * cos(
                                    radians(longitude) - radians("""+lon+""")
                                  ) + sin(
                                    radians("""+lat+""")
                                  ) * sin(
                                    radians(latitude)
                                  )
                                )
                              )* 1000 < """+range
        # print(query)
        events_all = pd.read_sql_query(query, conn)
        if events_all.empty:
            return jsonify({"events_ids": []})
        # print(events_all)
    except pd.io.sql.DatabaseError as e:
        logger.warning(e)
        return jsonify({'error': "sql query error"})
    model = ML(last_rows_user, events_all)
    result = model.get_events()
    # return events_all.to_json(orient="records")
    return jsonify({"events_ids": result})