import json

from flask import Flask, request, jsonify
from sqlalchemy import create_engine
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database
from local_settings import settings
import psycopg2
from flask_jwt_extended import JWTManager, jwt_required, get_jwt_identity
from config import Config
from kafka import KafkaProducer

# Flask init
app = Flask(__name__)
app.config.from_object(Config)
client = app.test_client()
# Postgresql init
db_path = f'postgresql://{settings["user"]}:{settings["password"]}@{settings["host"]}:{settings["port"]}/' \
          f'{settings["db_name"]}'
if not database_exists(db_path):
    create_database(db_path)
engine = create_engine(db_path)
session = scoped_session(sessionmaker(
    autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = session.query_property()

from models import *  # Для исправления закольцованного импортирования

Base.metadata.create_all(bind=engine)

# JWT init
jwt = JWTManager(app)

# Kafka init
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


@app.route("/api/v1/message", methods=['GET'])
@jwt_required()
def print_messages():  # Отладочная функция
    messages = Message.query.all()
    li_msg = []
    for message in messages:
        li_msg.append({
            'message_id': message.message_id,
            'text': message.text,
            'status': message.status,
        })
    return jsonify(li_msg)


@app.route("/api/v1/message", methods=["POST"])
@jwt_required()
def send_message():
    try:
        if 'text' not in request.json:
            raise Exception('Text parametr is missing')

        user_id = get_jwt_identity()
        new_message = Message(user_id=user_id, **request.json)
        new_message.status = 'review'
        session.add(new_message)
        session.commit()

        token = request.headers['Authorization']
        kafka_data = {'message_id': new_message.message_id, 'text': new_message.text, 'token': token}
        producer.send("messages", json.dumps(kafka_data))
        producer.flush()

        dct_msg = {
            'text': new_message.text,
            'status': new_message.status,
        }
    except Exception as e:
        return {'Error': str(e)}, 400
    return jsonify(dct_msg)


@app.route("/api/v1/message_confirmation", methods=["POST"])
@jwt_required()
def message_confirmation():
    message_data = request.json
    item = Message.query.filter(Message.message_id == message_data['message_id']).first()
    if message_data['success']:
        item.status = 'correct'
    else:
        item.status = 'blocked'
    session.commit()
    return str(message_data['success'])


@app.route("/api/v1/register", methods=['POST'])
def register():
    try:
        params = request.json

        if 'name' not in params:
            raise Exception('Name parameter is missing')
        elif 'password' not in params:
            raise Exception('Password parameter is missing')

        coincidence = User.query.filter(User.name == params['name']).first()
        if coincidence:
            raise Exception('User with the same name already exists!')

        user = User(name=params['name'], password=params['password'])
        session.add(user)
        session.commit()
        token = user.get_token()
    except Exception as e:
        return {'Error': str(e)}, 400
    return jsonify({'access_token': token})


@app.route('/api/v1/login', methods=['POST'])
def login():
    try:
        params = request.json

        if 'name' not in params:
            raise Exception('Name parameter is missing')
        elif 'password' not in params:
            raise Exception('Password parameter is missing')

        user = User.authenticate(params['name'], params['password'])
        token = user.get_token()
    except Exception as e:
        return {'Error': str(e)}, 400
    return {'access_token': token}


@app.teardown_appcontext
def shutdown_session():
    session.remove()


if __name__ == "__main__":
    app.run()
