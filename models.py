from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey
from my_api import db, Base
from flask_jwt_extended import create_access_token
from datetime import timedelta
from passlib.hash import bcrypt


class Message(Base):
    __tablename__ = 'rericom_messages'
    message_id = db.Column(db.Integer, primary_key=True)
    text = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False)
    user_id = db.Column(db.Integer, ForeignKey('users.id'))


class User(Base):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False, unique=True)
    password = db.Column(db.String, nullable=False)
    messages = relationship("Message")

    def __init__(self, name, password):
        self.name = name
        self.password = bcrypt.hash(password)

    def get_token(self, expire_time=0.5):
        expire_delta = timedelta(expire_time)
        token = create_access_token(identity=self.id, expires_delta=expire_delta)
        return token

    @classmethod
    def authenticate(cls, name, password):
        user = cls.query.filter(cls.name == name).one()
        if not user:
            raise Exception('User with this name does not exist')
        if not bcrypt.verify(password, user.password):
            raise Exception('Wrong password')
        return user
