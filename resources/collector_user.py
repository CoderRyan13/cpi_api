


from flask import request
from flask_restful import Resource
from flask_jwt_extended import JWTManager, create_access_token, get_jwt_identity, jwt_required

from models.collector_user import CollectorUserModel
from flask_bcrypt import generate_password_hash, check_password_hash
from marshmallow import ValidationError
from validators.errors import ServerError, Validation_Error
from validators.user import UserSchema

userSchema = UserSchema()

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

        if user.type != type:
            raise Validation_Error()
         
        # compare password with hash of password in db
        if check_password_hash(user.password, password):

            access_token = create_access_token(identity=user.id)
            return {
                'token': access_token, 
                'user': user.json(),
                }, 200

        else:
            raise Validation_Error()

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

    