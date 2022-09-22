from datetime import datetime
from marshmallow import Schema, fields, ValidationError, post_load, validate, validates_schema
from models.collector_assignment import AssignmentModel
from models.collector_outlet import CollectorOutletModel

from models.collector_substitution import SubstitutionModel
from models.collector_variety import CollectorVarietyModel

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')


def validate_time_period(val):
    assignment = AssignmentModel.find_by_id(val)
    if not assignment:
        raise ValidationError(f'Invalid assignment id: {val}')

    if assignment.status == 'inactive':
        raise ValidationError(f'Assignment is not in current time period: {val}')
    

class SubstitutionSchema(Schema):
    assignment_id = fields.Integer(validate=[validate.Range(min=1), validate_time_period], required=True, error_messages={"required": "assignment_id is required."})
    variety_id =  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "variety_id is required."})
    outlet_id=  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "outlet_id is required."})
    price= fields.Float(validate=validate.Range(min=0.01), required=True, error_messages={"required": "price is required."}) 
    comment= fields.String( required=True, error_messages={"required": "comment is required."}) 
    collected_at = fields.DateTime(format='%Y-%m-%d %H:%M:%S', required=True, error_messages={"required": "collected_at is required."})
    
    @validates_schema()
    def validate_substitution_schema(self, data, **kwargs):

        # validate assignment_id for existence
        assignment = AssignmentModel.find_by_id(data['assignment_id'])
        if not assignment:
            raise ValidationError({"message": 'assignment does not exist'})
      
        # validates outlet_id for existence    
        outlet = CollectorOutletModel.find_by_id(data['outlet_id'])
        if not outlet:
            raise ValidationError({'message': 'outlet is not valid!'})

        # validates variety_id for existence
        variety = CollectorVarietyModel.find_by_id(data['variety_id'])
        if not variety:
            raise ValidationError({'message': 'variety is not valid!'})
        
        # NOTE: This is a very important check to prevent duplication of assignments For Upcoming periods
        # It Validates that another assignment or substitution does not exist for the same outlet_id and variety_id

        _assignment_ = AssignmentModel.find_assignment_by_outlet_and_variety(data['outlet_id'], data['variety_id'])
        _substitution_ = AssignmentModel.find_substitution_by_outlet_and_variety(data['outlet_id'], data['variety_id'])


        print("ASSIGNMENT: ", _assignment_)
        print("SUBSTITUTION: ", _substitution_)


        if _assignment_:
            raise ValidationError({'message': 'An assignment for the same outlet and variety exists!- Assignment ID: {}'.format(_assignment_.id)})

        if _substitution_:
            
            # check if the substitution is being updated for the same assignment
            if _substitution_.parent_id == data['assignment_id']:
                return

            raise ValidationError({'message': 'A substitution for the same outlet and variety exists! - Assignment ID: {}'.format(_substitution_.parent_id)})


