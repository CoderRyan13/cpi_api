from datetime import datetime
from marshmallow import Schema, fields, ValidationError, post_load, validate, validates_schema
from models.collector_assignment import AssignmentModel
from validators.substitution import SubstitutionSchema

def validate_length(val):
    if len(val) > 255:
        raise ValidationError('Length of string is too long')

def validate_time_period(val):
    assignment = AssignmentModel.find_by_id(val)
    if not assignment:
        raise ValidationError(f'Invalid assignment id: {val}')

    if assignment.status == 'inactive':
        raise ValidationError(f'Assignment is not in current time period: {val}')
    

class AssignmentSchema(Schema):
    area_id =  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "area_id is required."})
    outlet_id =  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "outlet_id is required."})
    variety_id =  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "variety_id is required."})
    collector_id =  fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "collector_id is required."})
    from_outlet_id =  fields.Integer(validate=validate.Range(min=1), required=False, allow_none=True)
    is_headquarter =  fields.Boolean( required=True, error_messages={"required": "is_headquarter is required."})
    is_monthly =  fields.Boolean( required=True, error_messages={"required": "is_monthly is required."})
    

    @post_load
    def make_assignment(self, data, **kwargs):
        print(data)
        return AssignmentModel(**data)

    @validates_schema()
    def validate_assignment_schema(self, data, **kwargs):

        # validate assignment_id for existence
        existing_assignment = AssignmentModel.get_assignment_by_variety_outlet_collector(data['variety_id'], data['outlet_id'], data['collector_id'])

        if existing_assignment:

            # validate assignment_id for existence
            if existing_assignment.status == 'active':
                raise ValidationError({'message': f'Assignment Already Exists: {existing_assignment.id}'})


class AssignmentIdentitySchema(Schema):
    id = fields.Integer(validate=validate.Range(min=1), required=True, error_messages={"required": "id is required."})


class AssignmentPricesIdentitySchema(Schema):
    id = fields.Integer(validate=[validate.Range(min=1), validate_time_period], required=True, error_messages={"required": "id is required."})
    new_price = fields.Decimal(required=True, error_messages={"required": "new_price is required."})
    collected_at = fields.DateTime(format='%Y-%m-%d %H:%M:%S', required=True, error_messages={"required": "collected_at is required."})
    comment = fields.Str(required=False)
