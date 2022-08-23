from marshmallow import Schema, fields, ValidationError, post_load, validate, validates_schema

from models.collector_outlet import CollectorOutletModel

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')

class OutletSchema(Schema):
    est_name =  fields.String( required=True, error_messages={"required": "est_name is required."})
    address=  fields.String( required=True, error_messages={"required": "address is required."})
    note=  fields.String( required=True, error_messages={"required": "note is required."})
    phone=  fields.String( required=True, error_messages={"required": "phone is required."})
    area_id=  fields.Integer( validate=validate.Range(min=1), required=True, error_messages={"required": "area_id is required."})
    lat=  fields.Float( required=False, error_messages={"required": "lat is required."})
    long = fields.Float( required=False, error_messages={"required": "long is required."})

    @post_load
    def make_outlet(self, data, **kwargs):
        return CollectorOutletModel(**data)



class NewOutletSchema(Schema):
    mobile_id = fields.Integer( validate=validate.Range(min=1), required=True, error_messages={"required": "mobile_id is required."})
    est_name =  fields.String( required=True, error_messages={"required": "est_name is required."})
    address=  fields.String( required=True, error_messages={"required": "address is required."})
    note=  fields.String( required=True,  allow_none=True,  error_messages={"required": "note is required."})
    phone=  fields.String( required=True, error_messages={"required": "phone is required."})
    area_id=  fields.Integer( validate=validate.Range(min=1), required=True, error_messages={"required": "area_id is required."})
    lat=  fields.Float( required=False,  allow_none=True, error_messages={"required": "lat is required."})
    long = fields.Float( required=False, allow_none=True,  error_messages={"required": "long is required."})




class UpdateOutletSchema(Schema):
    mobile_id = fields.Integer( validate=validate.Range(min=1), required=True, error_messages={"required": "mobile_id is required."})
    est_name =  fields.String( required=True, error_messages={"required": "est_name is required."})
    address=  fields.String( required=True,  allow_none=True, error_messages={"required": "address is required."})
    note=  fields.String( required=True,  allow_none=True,  error_messages={"required": "note is required."})
    phone=  fields.String( required=True,  allow_none=True, error_messages={"required": "phone is required."})
    area_id=  fields.Integer( validate=validate.Range(min=1), required=True, error_messages={"required": "area_id is required."})
    lat=  fields.Float( required=False,  allow_none=True, error_messages={"required": "lat is required."})
    long = fields.Float( required=False, allow_none=True,  error_messages={"required": "long is required."})

    @validates_schema()
    def validate_outlet_schema(self, data, **kwargs):

        # validate assignment_id for existence
        outlet = CollectorOutletModel.find_by_id(data['mobile_id'])
        if not outlet:
            raise ValidationError('Identified Outlet does not exist')
      