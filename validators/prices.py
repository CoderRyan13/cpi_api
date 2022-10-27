from datetime import datetime
from marshmallow import Schema, fields, ValidationError, post_load, validate
from models.collector_price import CollectorPriceModel




def validate_assignment_price(val):

    assignment_price = CollectorPriceModel.find_by_assignment_id(val)
    
    if not assignment_price:
        raise ValidationError(f'Invalid assignment id: {val}')


class AssignmentPriceApprovalSchema(Schema):
    assignment_id = fields.Integer(validate=[validate.Range(min=1)], required=True, error_messages={"required": "assignment_id is required."})
    status = fields.String(required=True, validate=[validate.OneOf(choices=['approved', 'rejected'])], error_messages={"required": "status is required."})

class AssignmentExportSchema(Schema):
    time_period = fields.DateTime(format='%Y-%m-01', required=True, error_messages={"required": "time_period is required."})
    area_id = fields.Integer(required=True, validate=[validate.Range(min=1)],  error_messages={"required": "status is required."})
