from marshmallow import Schema, fields, ValidationError, post_load, validate

from models.collector_area import CollectorAreaModel

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')


class AreaSchema(Schema):
    name=  fields.String( required=True, error_messages={"required": "name is required."})
    district=  fields.String( required=True, error_messages={"required": "district is required."})
    
    @post_load
    def make_area(self, data, **kwargs):
        return CollectorAreaModel(**data)


