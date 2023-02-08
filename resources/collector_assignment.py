from flask_restful import Resource, request
from flask import send_file
from marshmallow import INCLUDE, ValidationError

from models.collector_assignment import AssignmentModel
from models.collector_price import CollectorPriceModel
from models.settings import SettingsModel, can_access_assignments
from validators.assignment import AssignmentIdentitySchema, AssignmentPricesIdentitySchema, AssignmentSchema
from validators.errors import NotFoundError, ServerError, Validation_Error
from flask_jwt_extended import jwt_required, get_jwt_identity

assignmentSchema = AssignmentSchema()
assignmentIdentitySchema = AssignmentIdentitySchema()
assignmentPricesIdentitySchema = AssignmentPricesIdentitySchema()

class CollectorAssignmentList(Resource):

    @jwt_required()
    def get(self):
        
        try:

            # get the query string parameters
            query = request.args.to_dict()

            # get the filters ready
            filter = {
                "search": query.get("search", ''),
                "page": query.get("page", None),
                "rows_per_page": query.get("rows_per_page", None),
                "sort_by": query.get("sort_by", None),
                "sort_desc": query.get("sort_desc", False),
                'collector_id': query.get('collector_id', None),
                'variety_id': query.get('variety_id', None),
                'region_id': query.get('region_id', None),
                'status': query.get('status', None)
            }

            # get the filters validated
            try:

                if filter['page'] and filter['rows_per_page'] :

                    int(filter['page'])
                    int(filter['rows_per_page'])

            except:
                raise Validation_Error("Invalid page or rows_per_page")


            # get the list of varieties and total
            result = AssignmentModel.find_all(filter)

            return {"total": result["count"], "assignments": result["assignments"]  }, 200
        
        except Exception as e:
            print(e)
            raise ServerError() 
        


    @jwt_required()
    @can_access_assignments
    def post(self):

        # loads the raw data from the body
        raw_assignments = request.get_json()

        # validates the data
        try:
            assignments = [ assignmentSchema.load(assignment) for assignment in raw_assignments ]

        except ValidationError as err:
            print(err)
            return {"errors": err.messages}, 400

        # saves the data
        for assignment in assignments:
            assignment.save_to_db('active')

        #respond to the client request    
        return {
            "message": "Assignment Submitted Successfully", 
            "assignments": [ assignment.json() for assignment in assignments]
        }, 201

        
class CollectorAssignment(Resource):

    @jwt_required()
    @can_access_assignments
    def get(self, id):
        assignment = AssignmentModel.find_by_id(id)
        if assignment:
            return assignment.json()
        return NotFoundError("Assignment")      

class ActivateAssignments(Resource):
    
    @jwt_required()
    @can_access_assignments
    def put(self, status):

        if status not in ["active", "inactive", "rejected"]:
            return {"message": "Invalid Status"}, 400

        assignments = request.get_json(silent=True)
        try:
            assignments = [ assignmentIdentitySchema.load(assignment,unknown=INCLUDE) for assignment in assignments ]
        except ValidationError as err:
            print(err)
            raise Validation_Error()

        changeAssignments = []

        for assignment in assignments:
            assignment = AssignmentModel.find_by_id(assignment["id"])
            if assignment:
                assignment.update_status(status)
                changeAssignments.append(assignment.json())
            
        return changeAssignments, 200

class UploadAssignmentsPrices(Resource):

    @jwt_required()
    @can_access_assignments
    def put(self):

        try:

            user_id= get_jwt_identity()

            assignments = request.get_json(silent=True)

            if not isinstance(assignments, list):
                raise ValidationError(["Missing assignments Data!"])

            assignments = [ assignmentPricesIdentitySchema.load(assignment,unknown=INCLUDE) for assignment in assignments ]
            print("Uploading assignment Prices...")

            changeAssignmentPrices = []

            for item in assignments:
                print(item)

                #verify if there is a price with this assignment id
                price = CollectorPriceModel.find_by_assignment_id(item["id"])

                if not price:

                    new_price = CollectorPriceModel.create_assignment_price({
                        "assignment_id": item["id"],
                        "price": item["new_price"],
                        "collected_at": item["collected_at"],
                        "comment": item["comment"],
                        "collector_id": user_id,
                        "flag": None if item["new_price"] > 0 else "IMPUTED"
                    })
                    changeAssignmentPrices.append(new_price.json())

                #if approved or inactive the assignment cannot be changed
                elif price:
                    if price.status not in ["approved"]:
                        price.update_price(item["new_price"], item["collected_at"], item["comment"], user_id, None if item["new_price"] > 0 else "IMPUTED")
                        changeAssignmentPrices.append(price.json())
                
                # clear assignment price for substitution if any
                AssignmentModel.clear_assignment_substitution(item["id"])
                
            return [assignmentPrice for assignmentPrice in changeAssignmentPrices], 200

        except ValidationError as err:
            print(err)
            raise Validation_Error()
        except Exception as err:
            print(err)
            raise ServerError()


# Used to get the assignments for the collector based on collector_id
class CollectorAssignmentListByCollector(Resource):
    @jwt_required()
    @can_access_assignments
    def get(self, collector_id):
        try:
            return AssignmentModel.find_by_collector(collector_id) 
        except Exception as err:
            print(err)
            raise ServerError()


# Used to get the assignments that were substituted and the varieties are new
class CollectorAssignmentSubstitutionsWithNewVariety(Resource):
    @jwt_required()
    @can_access_assignments
    def get(self):
        try:
            return AssignmentModel.find_substitutions_with_new_varieties() 
        except Exception as err:
            print(err)
            raise ServerError()


class CollectorAutomatedAssignmentList(Resource): 
    @jwt_required()
    def get(self):
        try:
            return AssignmentModel.find_automated_assignments() 
        except Exception as err:
            print(err)
            raise ServerError()

class CollectorHeadquarterAssignmentList(Resource):
    @jwt_required()
    def get(self):
        try:
            return AssignmentModel.find_headquarter_assignments() 
        except Exception as err:
            print(err)
            raise ServerError()


class CollectorAssignmentStatistics(Resource):
    @jwt_required()
    def get(self):
        try:
            return AssignmentModel.find_statistics() 
        except Exception as err:
            print(err)
            raise ServerError()


class CollectorOutletCoverageStats(Resource):
    @jwt_required()
    def get(self):
        try:
            return AssignmentModel.find_outlet_coverage_stats() 
        except Exception as err:
            print(err)
            raise ServerError()


class CollectorFilterAssignments(Resource):
    @jwt_required()
    def get(self):
        try:

            # get the query string parameters
            query = request.args.to_dict()

            # get the filters ready
            filter = {
                "search": query.get("search", ''),
                "page": query.get("page", None),
                "rows_per_page": query.get("rows_per_page", None),
                "sort_by": query.get("sort_by", None),
                "sort_desc": query.get("sort_desc", False),
                'collector_id': query.get('collector_id', None),
                'region_id': query.get('region_id', None),
                'price_status': query.get('price_status', None),
                'collection_process': query.get('collection_process', None),
                'requested_substitution_status': query.get('requested_substitution_status', None),
                'is_monthly_check' :  query.get('is_monthly_check', True)
            }

            # get the filters validated
            try:

                if filter['page'] and filter['rows_per_page'] :
                    int(filter['page'])
                    int(filter['rows_per_page'])


            except:
                raise Validation_Error("Invalid page or rows_per_page")

            if filter['price_status'] and filter['price_status'] not in [None, 'approved', 'missing', 'rejected', 'collected', 'pending']:
                    raise ValidationError(["Invalid price status"])

            if filter['collection_process'] and filter['collection_process'] not in [None, 'collected', 'substituted']:
                raise ValidationError(["Invalid price status"])

            if filter['requested_substitution_status'] and filter['requested_substitution_status'] not in [None, 'pending', 'approved', 'rejected']:
                raise ValidationError(["Invalid price status"])


            # get the list of varieties and total
            result = AssignmentModel.filter_current_assignments(filter)

            return {"total": result["count"], "assignments": result["assignments"]  }, 200

        except ValidationError as err:
            print(err)
            raise Validation_Error()
        
        except Exception as e:
            print(e)
            raise ServerError() 
        