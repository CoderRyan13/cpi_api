   
   
from datetime import datetime
from models.collector_assignment import AssignmentModel
from models.collector_price import CollectorPriceModel
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_restful import Resource

class SyncAutomatedAssignments(Resource):
    
    # used to sync automated assignments by finding the assignment it depend on and copying
    # the assignment collection price or substitution if any. Note (It clears the price or substitution if any!)
    @jwt_required()
    def put(self):

        user_id = get_jwt_identity()

        missing_collection = []

        # find all automated assignments
        automated_assignments = AssignmentModel.find_automated_assignments()

        # loop through the automated assignments
        for automated_assignment in automated_assignments:

            # find which assignment this automated assignment depends on
            from_assignment_id = automated_assignment['from_assignment_id']

            # find which assignment this automated assignment depends on
            from_assignment = AssignmentModel.find_by_id(from_assignment_id)
            from_assignment_substitution = AssignmentModel.find_assignment_substitution(from_assignment.id)

            # Lets verify If a price has already been set to the automated assignment

            # Check if the automated assignment has been substituted before to clear it which also clears the prices
            if automated_assignment['substitution']:
                AssignmentModel.clear_assignment_substitution(automated_assignment["id"])
                
            # check if the automated assignment has a price already to clear it 
            if automated_assignment['new_price']:
                CollectorPriceModel.clear_assignment_price(automated_assignment['id'])


            # First Check if the donor assignment has a substitution
            if from_assignment_substitution:

                if from_assignment_substitution.price.status == 'approved':

                    # Since approved then a substitution needs to be created for the automated assignment 
                    automated_substitution = AssignmentModel.save_substitution({
                        'assignment_id': automated_assignment["id"],
                        'outlet_id': automated_assignment["outlet_id"],
                        'variety_id': from_assignment_substitution.variety_id,
                        "price": from_assignment_substitution.price.price,
                        "comment": from_assignment_substitution.price.comment,
                        "collected_at": datetime.now(),
                        "from_assignment_id": from_assignment_substitution.id
                    })

                    # approve the substitution price
                    automated_substitution.price.update_status( 'approved', user_id)
    
                    # skip the rest of actions
                    continue

                
                elif from_assignment_substitution.price.status == 'rejected': 

                    # Create a new price record for the automated assignment
                    automated_substitution_price = CollectorPriceModel.create_assignment_price({
                        "assignment_id": automated_assignment['id'],
                        "comment": from_assignment_substitution.price.comment,
                        "price": from_assignment_substitution.price.price,
                        "collected_at": datetime.now(),
                        "collector_id": automated_assignment['collector_id']
                    })
                   
                    # reject the automated assignment price
                    automated_substitution_price.update_status('rejected', user_id)

                else:
                    missing_collection.append(from_assignment.json())

                # skip the rest of actions
                continue
    
            elif from_assignment.price:

                # verify if the assignment price has been verified by hq
                if from_assignment.price.status in ['approved', 'rejected']:

                    # create a new price record for the automated assignment
                    automated_assignment_price = CollectorPriceModel.create_assignment_price({
                        "assignment_id": automated_assignment['id'],
                        "comment": from_assignment.price.comment,
                        "price": from_assignment.price.price,
                        "collected_at": datetime.now(),
                        "collector_id": automated_assignment['collector_id']
                    })
                   
                    # approve or reject the automated assignment price
                    automated_assignment_price.update_status(from_assignment.price.status, user_id)

                else:
                    missing_collection.append(from_assignment.json())

                # skip the rest of actions
                continue
            
            else:
                
                # if the Donor assignment has no price or substitution then add it to the missing collection
                missing_collection.append(from_assignment.json())

        #  Return the Unique Donor assignments that need to be collected or verified by the HQ
        missing_collection = list({collection['id']:collection for collection in missing_collection}.values())

        return missing_collection, 200
