from flask_restful import Resource
from models.collector_assignment import AssignmentModel

from models.collector_working_price import WorkingPriceModel
from flask_jwt_extended import jwt_required


class WorkingPrice(Resource):
    
    
    # @jwt_required()
    def get(self):

        # First the application needs to verify if all assignments have a price status of Approved or Rejected
        if WorkingPriceModel.verify_all_assignment_approval():

            # Clean the Current time period working Prices before Copying Prices
            WorkingPriceModel.clean_current_time_period()

            # get all the assignments
            assignments_data = AssignmentModel.filter_current_assignments({'search': ''})

            # create a working price history for the assignments for this time period
            WorkingPriceModel.save_assignments_to_db(assignments_data["assignments"])
            
            return { 'total': len(assignments_data["assignments"]), "success": True}

        else:
            
            return { "success": False , "message": "All assignments need to have a price status of Approved or Rejected"} , 400


