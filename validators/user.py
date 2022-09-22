from marshmallow import Schema, fields, validate, validates_schema, ValidationError

from models.collector_user import CollectorUserModel


def validate_area_id(val):
    if not val:
        if val < 1 or val > 11:
            raise ValidationError({"message": "Area does not exist!"})


class UserSchema(Schema):
    name =  fields.String( required=True, error_messages={"required": "name is required."})
    email =  fields.String( required=True, error_messages={"required": "email is required."})
    username =  fields.String( required=True, error_messages={"required": "username is required."})
    type =  fields.String( required=True, error_messages={"required": "type is required."})
    area_id =  fields.Integer( validate=[validate_area_id] , allow_none=True, required=False, error_messages={"required": "type is required."} )
    id =  fields.Integer( validate=validate.Range(min=1), required=True, error_messages={"required": "id is required."})


    @validates_schema()
    def validate_user_schema(self, data, **kwargs):

        if data['type'] == 'collector':
            if data.get('area_id', None) == None: 
                raise ValidationError({"message": "Area is Required for Collectors!"})

        # validate user for existence
        user_by_id = CollectorUserModel.find_by_id(data['id'])
        if not user_by_id:
            raise ValidationError({"message": 'User does not exist'})

        # validate the username 
        user_by_username = CollectorUserModel.find_by_username(data['username'])
        if user_by_username:
            if user_by_username.id != data['id']:
                raise ValidationError({"message": f'Another user exist with same Username: {data["username"]}'})

         # validate the username 
        user_by_email = CollectorUserModel.find_by_email(data['email'])
        if user_by_email:
            if user_by_email.id != data['id']:
                raise ValidationError({"message": f'Another user exist with same Email: {data["email"]}'})