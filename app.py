from flask import Flask
from flask_restful import Api
from flask_bcrypt import Bcrypt
from flask_jwt_extended import JWTManager
from data_loaders.areas import AreasRawDataLoader
from data_loaders.configure_automated_assignments import ConfigureAutomatedAssignments
from data_loaders.configure_portal_varieties import ConfigurePortalVarieties
from data_loaders.outlets import OutletsRawDataLoader
from data_loaders.upload_excel import UploadExcel
from data_loaders.varieties import VarietiesRawDataLoader
from db import db
from resources.collector import Collectors
from resources.collector_area import CollectorArea, CollectorAreaList
from resources.collector_assignment import ActivateAssignments, CollectorAssignment, CollectorAssignmentList, CollectorAssignmentListByCollector, UploadAssignmentsPrices
from resources.collector_outlet import CollectorOutlet, CollectorOutletList, CollectorOutletListByCollector
from resources.collector_requested_substitution import CollectorRequestSubstitution
from resources.collector_variety import CollectorVariety, CollectorVarietyList, CollectorVarietyListByCollector
from resources.collector_product import CollectorProduct, CollectorProductList

# from resources.outlet import Outlet, OutletList
from resources.collector_substitution import SubstitutionList 
from resources.cpi_variety import CPIVariety, CPIVarietyList
from werkzeug.exceptions import HTTPException
from flask_cors import CORS

from resources.time_period import TimePeriods
from resources.collector_user import ChangePassword, Login
from data_loaders.products import ProductsRawDataLoader
from data_loaders.assignments import AssignmentsRawDataLoader

app = Flask(__name__) 
CORS(app)
api = Api(app)

bcrypt = Bcrypt(app)

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


################## THE RESOURCES TO THE API ####################


# ---------------------- CPI VARIETIES ENDPOINTS --------------------
api.add_resource( CPIVariety, '/cpi-varieties/<string:id>')
api.add_resource( CPIVarietyList, '/cpi-varieties')


# ---------------------- PORTAL SUBSTITUTION ENDPOINTS --------------------
api.add_resource( SubstitutionList , '/substitutions')
api.add_resource(CollectorRequestSubstitution, '/request-substitutions')


# ---------------------- PORTAL ASSIGNMENTS ENDPOINTS --------------------
api.add_resource( CollectorAssignment, '/assignments/<string:id>')
api.add_resource( CollectorAssignmentList , '/assignments')
api.add_resource( ActivateAssignments, '/activate-assignments/<string:status>')
api.add_resource( CollectorAssignmentListByCollector, '/assignments/user-assignments/<string:collector_id>') 
api.add_resource( UploadAssignmentsPrices , '/assignments-upload')


# ---------------------- PORTAL VARIETIES ENDPOINTS --------------------
api.add_resource( CollectorVariety, '/varieties/<string:id>')
api.add_resource( CollectorVarietyList, '/varieties')
api.add_resource( CollectorVarietyListByCollector, '/varieties/user-varieties/<int:collector_id>') 


# ---------------------- PORTAL OUTLETS ENDPOINTS --------------------
api.add_resource( CollectorOutletList, '/outlets') 
api.add_resource( CollectorOutlet, '/outlets/<string:id>') 
api.add_resource( CollectorOutletListByCollector, '/outlets/user-outlets/<int:collector_id>') 


# ---------------------- PORTAL PRODUCTS ENDPOINTS --------------------------
api.add_resource( CollectorProductList, '/products')
api.add_resource( CollectorProduct, '/products/<string:id>')


# ---------------------- PORTAl AREA ENDPOINTS
api.add_resource( CollectorAreaList, '/areas') 
api.add_resource( CollectorArea, '/areas/<string:id>') 


# ---------------------- GENERAL FORM DATA GETTERS ----------------------
api.add_resource( Collectors, '/collectors')
api.add_resource( TimePeriods, '/time_periods')


# ---------------------- AUTHENTICATION ----------------------
api.add_resource( Login, '/login')
api.add_resource( ChangePassword, '/change-password/<int:id>')


# ---------------------- DATA LOADERS FROM SIMA ----------------------
api.add_resource( ProductsRawDataLoader, "/products-dataloader" )
api.add_resource( VarietiesRawDataLoader, '/varieties-dataloader' )
api.add_resource( AssignmentsRawDataLoader, '/assignments-dataloader' )
api.add_resource( OutletsRawDataLoader, '/outlets-dataloader' )
api.add_resource( AreasRawDataLoader, '/areas-dataloader' )


# ---------------------- PORTAL DATA CONFIGURATION --------------------
api.add_resource( UploadExcel, '/read-excel')
api.add_resource( ConfigurePortalVarieties, '/configure-portal-varieties/<string:filename>' )
api.add_resource( ConfigureAutomatedAssignments, '/configure-automated-assignments/<string:filename>' )


#RUNS THE API
if __name__ == '__main__':
    app.run( host='0.0.0.0', port=8080, debug=True )


