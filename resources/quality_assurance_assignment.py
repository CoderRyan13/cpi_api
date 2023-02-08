

from flask_restful import Resource, request
from marshmallow import ValidationError

from models.quality_assurance_assignment import QualityAssuranceAssignmentModel
from validators.errors import ServerError, Validation_Error
from validators.assignment import AssignmentPricesIdentitySchema
from marshmallow import INCLUDE, ValidationError
from flask_jwt_extended import jwt_required, get_jwt_identity
from models.settings import  can_access_assurance_assignments


assignmentPricesIdentitySchema = AssignmentPricesIdentitySchema()

class CollectorQualityAssuranceAssignment(Resource):

   

    
    def get(self):
        try: 
            query = request.args.to_dict()
            return QualityAssuranceAssignmentModel.getAssuranceAssignments(query)
        except Exception as e:
            print(e)
            raise ServerError()
    

    @jwt_required()
    @can_access_assurance_assignments
    def put(self):

        try:

            assignments = request.get_json(silent=True)

            if not isinstance(assignments, list):
                raise ValidationError(["Missing assignments Data!"])

            assignments = [ assignmentPricesIdentitySchema.load(assignment,unknown=INCLUDE) for assignment in assignments ]

            return QualityAssuranceAssignmentModel.updateAssignmentPrice(assignments), 200

        except ValidationError as err:
            print(err)
            raise Validation_Error()
        except Exception as err:
            print(err)
            raise ServerError()

class CollectorPortalQualityAssuranceAssignment(Resource):

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
                'hq_id': query.get('hq_id', None),
                'region_id': query.get('region_id', None),
            }

            # get the filters validated
            try:

                if filter['page'] and filter['rows_per_page'] :
                    int(filter['page'])
                    int(filter['rows_per_page'])


            except:
                raise Validation_Error("Invalid page or rows_per_page")

            # get the list of assignments and total
            result = QualityAssuranceAssignmentModel.getPortalAssuranceAssignments(filter)

            return {"total": result["count"], "assignments": result["assignments"]  }, 200
            
        except Exception as e:
            print(e)
            raise ServerError()


    def post(self):

        try: 
            
            body = request.get_json(silent=True)
            print(body)
            area_id = body.get("area_id", None)
            hq_id = body.get("hq_id", None)

            if area_id == None or hq_id  == None:
                raise ValidationError("Missing area_id or hq_id")
            
            result = QualityAssuranceAssignmentModel.generateAssuranceAssignments(area_id, hq_id)

            if result.get('error', None):
                return result, 400
            
            return result

        except ValidationError as e:
            print(e)
            raise Validation_Error()
        except Exception as e:
            print(e)
            raise ServerError()


    def put(self):

        try: 
            
            body = request.get_json(silent=True)
            area_id = body.get("area_id", None)
            hq_id = body.get("hq_id", None)

            if area_id == None or hq_id  == None:
                raise ValidationError("Missing area_id or hq_id")
            
            return QualityAssuranceAssignmentModel.activateAssuranceAssignments(area_id, hq_id)

        except ValidationError as e:
            print(e)
            raise Validation_Error()
        except Exception as e:
            print(e)
            raise ServerError()

