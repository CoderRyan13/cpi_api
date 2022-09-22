from marshmallow import Schema, fields, ValidationError, post_load, validate
from models.collector_product import CollectorProductModel

from models.collector_variety import CollectorVarietyModel

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')


class VarietySchema(Schema):
    name =  fields.String(required=True, error_messages={"required": "name is required."})
    code=  fields.String( required=True, error_messages={"required": "code is required."})
    product_id=  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "product_id is required."})
    
    @post_load
    def make_variety(self, data, **kwargs):
        return CollectorVarietyModel(**data)


def validate_code(val):
    product = CollectorProductModel.find_by_code(val)
    if not product:
        raise ValidationError('Product not found')

class NewVarietySchema(Schema):
    mobile_id = fields.Integer( validate=validate.Range(min=1), required=False)
    code =  fields.String( validate=[validate_code], required=True, error_messages={"required": "code is required."})
    name=  fields.String( required=True, error_messages={"required": "name is required."})
   

