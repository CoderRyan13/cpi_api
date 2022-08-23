from marshmallow import Schema, fields, validate, ValidationError

from models.collector_assignment import AssignmentModel


def validate_assignment(val):
    if not AssignmentModel.find_by_id(val):
        raise ValidationError('Assignment not found')

class RequestSubstitutionSchema(Schema):
    assignment_id = fields.Integer(validate=[validate.Range(min=1), validate_assignment], required=True, error_messages={"required": "assignment_id is required."})