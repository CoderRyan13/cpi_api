


from flask import request
from flask_restful import Resource
from flask_jwt_extended import JWTManager, create_access_token, get_jwt_identity, jwt_required, get_jwt

from models.collector_user import CollectorUserModel
from flask_bcrypt import generate_password_hash, check_password_hash
from marshmallow import ValidationError
from validators.errors import ServerError, Validation_Error
from validators.user import UserSchema

from db import db

from flask.views import MethodView
from flask_smorest import abort
from passlib.hash import pbkdf2_sha256

from blocklist import BLOCKLIST

userSchema = UserSchema()

class Users(MethodView):
    @jwt_required()
    def get(self):
        return [ users.json() for users in CollectorUserModel.find_all() ]

    #@jwt_required
    def post(self):     
        raw_data = request.get_json(silent=True)
        #print(raw_data)

        #checks to see if a user with the provided username already exists in database
        if CollectorUserModel.query.filter(CollectorUserModel.username == raw_data.get('username', None)).first():
            abort(409, message="A user with that username already exists.")

        user = CollectorUserModel(
            name=raw_data.get('name', None),
            email=raw_data.get('email', None),
            username=raw_data.get('username', None),
            password=generate_password_hash(raw_data.get('password', None), None),
            area_id=raw_data.get('area_id', None),
            type=raw_data.get('type', None),
            status=raw_data.get('status', None)
        )
        db.session.add(user) #adds the new user
        db.session.commit()

        return {"message": "User created successfully!"}, 201
    
class UsersID(MethodView):
    @jwt_required()
    def get(self, id):
        user = CollectorUserModel.find_by_id(id)
        return user.json()
    
    @jwt_required()
    def delete(self, id):
        user = CollectorUserModel.find_by_id(id)

        db.session.delete(user) #deletes a user
        db.session.commit()

        return {"message": "User deleted."}
    
    # @jwt_required()
    # def put(self, id):
    #     raw_data = request.get_json(silent=True)
    #     user = CollectorUserModel.find_by_id(id)

    #     if user:
    #         user.name=raw_data.get('name', None),
    #         user.email=raw_data.get('email', None),
    #         user.username=raw_data.get('username', None),
    #         user.password=generate_password_hash(raw_data.get('password', None), None),
    #         user.area_id=raw_data.get('area_id', None),
    #         user.type=raw_data.get('type', None)
    #     else:
    #         user = CollectorUserModel(id=id, **raw_data)

    #     db.session.add(user)
    #     db.session.commit()

    #     return user

        

class Login(Resource): 

    def post(self):

        raw_data = request.get_json(silent=True)
        print(raw_data)
        username = raw_data.get('username', None)
        password = raw_data.get('password', None)
        type = raw_data.get('type', None)

        if username is None or password is None or type not in ["collector", "HQ"]:
            raise Validation_Error()

        print(username)
        user = CollectorUserModel.find_by_username(username)


        if user is None:
            raise Validation_Error()

        print(user)

        if user.type == 'collector' and type == 'HQ':
            raise Validation_Error()
         
        # compare password with hash of password in db
        if check_password_hash(user.password, password):  
        #if pbkdf2_sha256.verify(password, user.password):

            access_token = create_access_token(identity=user.id)
            print(access_token)
            return {
                'token': access_token, 
                'user': user.json(),
                }, 200

        else:
            
            raise Validation_Error()
        

class Logout(MethodView):
    @jwt_required()
    def post(self):
        jti = get_jwt()["jti"]
        BLOCKLIST.add(jti)
        return {"message": "Successfully logged out."}


class VerifyToken(Resource):

    @jwt_required()
    def get(self, type):
        
        try:
            print(type)

            if type not in ["collector", "HQ"]:
                raise Validation_Error()

            
            print("Verifying token...")
            user_id = get_jwt_identity()
            user = CollectorUserModel.find_by_id(user_id)
            if user.type != type:
                raise Validation_Error()
            return user.json(), 200
        except Exception as err:
            print(err)
            raise Validation_Error()
        
class ChangePassword(Resource):
    @jwt_required()
    def put(self, id):
        
        raw_data = request.get_json(silent=True)
        password = raw_data.get('password', None)

        if password is None:
            raise Validation_Error()

        user = CollectorUserModel.find_by_id(id)

        if user is None:
            raise Validation_Error()

        # Hashing the password
        hashed = generate_password_hash(password).decode("utf-8")
        #hashed = pbkdf2_sha256.hash(password)

        # update password with hashed password in db
        user.update_password(hashed)
        return {"message": "password changed"}, 200

class User(Resource):

    @jwt_required()
    def put(self):
        try: 

            # get raw Data
            raw_data = request.get_json(silent=True)

            print(raw_data)
        
            # validate input and update data
            user_data = userSchema.load(raw_data)
            user = CollectorUserModel.find_by_id(user_data['id'])
            user.update(user_data)
            return user.json(), 201

        except ValidationError as err:
            print("Error", err)
            return  {'errors': err.messages}, 400
        except Exception as err:
            print(err)
            raise ServerError()
        
    