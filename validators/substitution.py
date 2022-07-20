from marshmallow import Schema, fields, ValidationError, post_load, validate, validates_schema
from models.collector_outlet import CollectorOutletModel

from models.collector_substitution import SubstitutionModel
from models.collector_variety import CollectorVarietyModel

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')


class SubstitutionSchema(Schema):
    variety_id =  fields.Integer(validate=validate.Range(min=0), required=True, error_messages={"required": "variety_id is required."})
    outlet_id=  fields.Integer(validate=validate.Range(min=0), required=True, error_messages={"required": "outlet_id is required."})
    
    
    @validates_schema()
    def validate_substitution_schema(self, data, **kwargs):
        print(self)
        print(data)
        # validates outlet_id for existence    
        outlet = CollectorOutletModel.find_by_id(data['outlet_id'])
        if not outlet:
            raise ValidationError('outlet_id is not valid!')

        # validates variety_id for existence
        variety = CollectorVarietyModel.find_by_id(data['variety_id'])
        if not variety:
            raise ValidationError('variety_id is not valid!')
        
        
    @post_load
    def make_substitution(self, data, **kwargs):
        return SubstitutionModel(**data)


