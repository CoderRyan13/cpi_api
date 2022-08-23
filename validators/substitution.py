from marshmallow import Schema, fields, ValidationError, post_load, validate, validates_schema
from models.collector_assignment import AssignmentModel
from models.collector_outlet import CollectorOutletModel

from models.collector_substitution import SubstitutionModel
from models.collector_variety import CollectorVarietyModel

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')

class SubstitutionSchema(Schema):
    assignment_id = fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "assignment_id is required."})
    variety_id =  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "variety_id is required."})
    outlet_id=  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "outlet_id is required."})
    price= fields.Float(validate=validate.Range(min=0.01), required=True, error_messages={"required": "price is required."}) 
    collected_at = fields.DateTime(format='%Y-%m-%d %H:%M:%S', required=True, error_messages={"required": "collected_at is required."})
    
    @validates_schema()
    def validate_substitution_schema(self, data, **kwargs):

        # validate assignment_id for existence
        assignment = AssignmentModel.find_by_id(data['assignment_id'])
        if not assignment:
            raise ValidationError('assignment_id does not exist')
      
        # validates outlet_id for existence    
        outlet = CollectorOutletModel.find_by_id(data['outlet_id'])
        if not outlet:
            raise ValidationError('outlet_id is not valid!')

        # validates variety_id for existence
        variety = CollectorVarietyModel.find_by_id(data['variety_id'])
        if not variety:
            raise ValidationError('variety_id is not valid!')
        
        


