from http.client import BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND
from flask import Flask, jsonify, make_response
from flask_restful import Api
from flask_jwt_extended import JWTManager
from db import db
from resources.collector import Collectors
from resources.collector_area import CollectorArea, CollectorAreaList
from resources.collector_assignment import ActivateAssignments, CollectorAssignment, CollectorAssignmentList, CollectorAssignmentListByCollector
from resources.collector_outlet import CollectorOutlet, CollectorOutletList, CollectorOutletListByCollector
from resources.collector_variety import CollectorVariety, CollectorVarietyList, CollectorVarietyListByCollector
from resources.collector_product import CollectorProduct, CollectorProductList

# from resources.outlet import Outlet, OutletList
from resources.collector_substitution import SubstitutionList 
from resources.cpi_variety import CPIVariety, CPIVarietyList
from werkzeug.exceptions import HTTPException
from flask_cors import CORS, cross_origin

from resources.data_loader import AreasDataLoader, AssignmentDataLoader, OutletsDataLoader, ProductsDataLoader, VarietiesDataLoader
from resources.time_period import TimePeriods
from resources.users import Login

app = Flask(__name__) 
CORS(app)
api = Api(app)

app.config["JWT_SECRET_KEY"] = "super-secret" 
jwt = JWTManager(app)


#CONFIGURES THE DATA BASE CONNECTION
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:mysql@localhost/cpi-collector'
app.config['SQLALCHEMY_BINDS'] = {'cpi': 'mysql://sib_gian:Letmein123!@192.168.0.3/cpi2' }

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

@app.errorhandler(HTTPException)
def handle_root_exception(error):
    return {
        'message': error.message if hasattr(error, 'message') else 'An error occurred',
        'status': error.code,
        'description': error.description,
    }, error.code

#ADDS THE RESOURCES TO THE API
api.add_resource( SubstitutionList , '/substitutions')

api.add_resource( CollectorAssignment, '/assignments/<string:id>')
api.add_resource( CollectorAssignmentList , '/assignments')
api.add_resource( ActivateAssignments, '/activate-assignments/<string:status>')
api.add_resource( CollectorAssignmentListByCollector, '/assignments/user-assignments/<string:collector_id>') 


api.add_resource( CPIVariety, '/cpi-varieties/<string:id>')
api.add_resource( CPIVarietyList, '/cpi-varieties')

api.add_resource( CollectorVariety, '/varieties/<string:id>')
api.add_resource( CollectorVarietyList, '/varieties')
api.add_resource( CollectorVarietyListByCollector, '/varieties/user-varieties/<int:collector_id>') 

api.add_resource( CollectorOutletList, '/outlets') 
api.add_resource( CollectorOutlet, '/outlets/<string:id>') 
api.add_resource( CollectorOutletListByCollector, '/outlets/user-outlets/<int:collector_id>') 

api.add_resource( CollectorProductList, '/products')
api.add_resource( CollectorProduct, '/products/<string:id>')

api.add_resource( CollectorAreaList, '/areas') 
api.add_resource( CollectorArea, '/areas/<string:id>') 

api.add_resource( ProductsDataLoader, '/products-dataloader' )
api.add_resource( VarietiesDataLoader, '/varieties-dataloader' )
api.add_resource( AssignmentDataLoader, '/assignments-dataloader' )
api.add_resource( OutletsDataLoader, '/outlets-dataloader' )
api.add_resource( AreasDataLoader, '/areas-dataloader' )

api.add_resource( Collectors, '/collectors')
api.add_resource( TimePeriods, '/time_periods')

api.add_resource( Login, '/login')


#RUNS THE API
if __name__ == '__main__':
    app.run(host='192.168.0.114', port=8080, debug=True)