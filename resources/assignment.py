

from flask_restful import Resource, request
from marshmallow import ValidationError

from models.collector_assignment import AssignmentModel
from validators.assignment import AssignmentSchema
from validators.errors import ServerError

assignmentSchema = AssignmentSchema()

class AssignmentList(Resource):

    def get(self):
        try: 
            
            return [ assignment.json() for assignment in AssignmentModel.find_all() ]
        except Exception as e:
            raise ServerError()

    def put(self):
        # loads the raw data from the body
        raw_data = request.get_json()

        # validates the data
        try:
            assignments = [ assignmentSchema.load(assignment) for assignment in raw_data["assignments"] ]
        except ValidationError as err:
            print(err)
            return {"errors": err.messages}, 400
      
        # saves the data
        for assignment in assignments:
            print("saving", assignment.substitution)
            assignment.save_to_db()

        #respond to the client request    
        return {
            "message": "Assignments created", 
            "assignments": [ assignment.json() for assignment in assignments]
        }, 201


class Assignment(Resource):
    def get(self, id):
        assignment = AssignmentModel.find_by_id(id)
        if assignment:
            return assignment.json()
        return {'message': 'Assignment not found'}, 404

    def put(self, id):
        return {'assignment': 'CPI Collector'}

    def delete(self, id):
        return {'assignment': 'CPI Collector'}


class AssignmentLoader(Resource):
    
    def get(self):
        loaded_assignments = AssignmentModel.load_period_assignment()
        if len(loaded_assignments) >= 0:
            return [assignment.json() for assignment in loaded_assignments]
        return {'message': 'Assignment Failed to loaded'}


    