


from flask import request
from flask_restful import Resource
from flask_jwt_extended import JWTManager, create_access_token

from validators.errors import Validation_Error
from db import cpi_db

class Login(Resource): 

    def post(self):
        raw_data = request.get_json(silent=True)
        username = raw_data.get('username', None)
        password = raw_data.get('password', None)
        if username is None or password is None:
            raise Validation_Error()

        query = """
                SELECT id, 
                    email, 
                    username,
                    (
                        SELECT area_id
                        FROM user JOIN user_outlet_product ON user.id = user_outlet_product.user_id
                        JOIN outlet_product ON user_outlet_product.outlet_product_id = outlet_product.id
                        JOIN outlet ON outlet_product.outlet_id = outlet.id
                        WHERE u.id = user.id
                        ORDER BY user_outlet_product.id DESC
                        LIMIT 1
                    ) as area_id
                FROM user as u WHERE username = %s AND password = %s
            """
        cpi_db.execute(query, (username, password))
        user = cpi_db.fetchone()

        if user is None:
            raise Validation_Error()

        access_token = create_access_token(identity=username, )
        return {'token': access_token, "id": user[0], "email": user[1], "username": user[2] , "area_id": user[3] }, 200
