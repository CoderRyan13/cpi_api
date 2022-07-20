from marshmallow import Schema, fields, ValidationError, post_load, validate
from models.collector_assignment import AssignmentModel
from validators.substitution import SubstitutionSchema

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')

class AssignmentSchema(Schema):
    outlet_product_variety_id =  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "outlet_product_variety_id is required."})
    comment=  fields.Str( validate=validate_length, required=True, error_messages={"required": "comment is required."})
    new_price=  fields.Decimal(required=True, error_messages={"required": "new_price is required."})
    previous_price=  fields.Decimal(required=True, error_messages={"required": "previous_price is required."})
    time_period=  fields.Date(required=True, error_messages={"required": "time_period is required."})
    outlet_id=  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "outlet_id is required."})
    outlet_name=  fields.Str(validate=validate_length, required=True, error_messages={"required": "outlet_name is required."})
    code=  fields.Str(validate=validate_length, required=True, error_messages={"required": "code is required."})
    collector_id=  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "collector_id is required."})
    collector_name=  fields.Str(validate=validate_length, required=True, error_messages={"required": "collector_name is required."})
    substitution= fields.Nested( SubstitutionSchema, required=False)

    @post_load
    def make_assignment(self, data, **kwargs):
        print(data)
        return AssignmentModel(**data)


class AssignmentIdentitySchema(Schema):
    id = fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "id is required."})

   