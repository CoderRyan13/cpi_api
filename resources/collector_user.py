


from flask import request
from flask_restful import Resource
from flask_jwt_extended import JWTManager, create_access_token
from models.collector_user import CollectorUserModel
from flask_bcrypt import generate_password_hash, check_password_hash

from validators.errors import Validation_Error
from db import cpi_db

class Login(Resource): 

    def post(self):

        raw_data = request.get_json(silent=True)
        username = raw_data.get('username', None)
        password = raw_data.get('password', None)
        if username is None or password is None:
            raise Validation_Error()

        user = CollectorUserModel.find_by_username(username)

        if user is None:
            raise Validation_Error()
         
        # compare password with hash of password in db
        if check_password_hash(user.password, password):
            access_token = create_access_token(identity=username)
            return {
                'token': access_token, 
                "id": user.id, 
                "email": user.email, 
                "username": user.username , 
                "area_id": user.area_id 
                }, 200

        else:
            raise Validation_Error()

        
class ChangePassword(Resource):
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

        # update password with hashed password in db
        user.update_password(hashed)
        return {"message": "password changed"}, 200