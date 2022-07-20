from marshmallow import Schema, fields, ValidationError, post_load, validate

from models.collector_outlet import CollectorOutletModel

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')

class OutletSchema(Schema):
    est_name =  fields.String( required=True, error_messages={"required": "est_name is required."})
    address=  fields.String( required=True, error_messages={"required": "address is required."})
    phone=  fields.String( required=True, error_messages={"required": "phone is required."})
    area_id=  fields.Integer( validate=validate.Range(min=1), required=True, error_messages={"required": "area_id is required."})
    
    @post_load
    def make_outlet(self, data, **kwargs):
        return CollectorOutletModel(**data)


