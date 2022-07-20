from marshmallow import Schema, fields, ValidationError, post_load, validate

from models.collector_product import CollectorProductModel

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')


def validate_status(val):
    if val not in ["active", "inactive"]:
        raise ValidationError('Status must be either active or inactive')


class ProductSchema(Schema):
    code =  fields.String( required=True, error_messages={"required": "code is required."})
    description=  fields.String( required=True, error_messages={"required": "description is required."})
    status=  fields.String( validate=validate_status, required=True, error_messages={"required": "status is required."})
    
    @post_load
    def make_product(self, data, **kwargs):
        return CollectorProductModel(**data)


