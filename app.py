from datetime import timedelta
from flask import Flask
from flask_restful import Api
from flask_bcrypt import Bcrypt
from flask_jwt_extended import JWTManager
from data_loaders.configure_assignments import ConfigureAssignments
from data_loaders.sync_automated_assignments import SyncAutomatedAssignments
from data_loaders.upload_excel import UploadExcel
from db import db
from env import JWT_SECRET_KEY, PORTAL_CONFIG_STRING, PORTAL_CONFIGS
from imports.assignments import AssignmentDataImport
from imports.outlets import OutletsDataImport
from imports.prices import PricesDataImport
from imports.products import ProductsDataImport
from imports.varieties import VarietiesDataImport
from resources.collector import Collectors
from resources.collector_area import CollectorArea, CollectorAreaList
from resources.collector_assignment import ActivateAssignments, CollectorAssignment, CollectorAssignmentList, CollectorAssignmentListByCollector, CollectorAssignmentStatistics, CollectorAssignmentSubstitutionsWithNewVariety, CollectorAutomatedAssignmentList, CollectorFilterAssignments, CollectorHeadquarterAssignmentList, CollectorOutletCoverageStats, UploadAssignmentsPrices
from resources.collector_outlet import CollectorOutlet, CollectorOutletByArea, CollectorOutletList, CollectorOutletListByCollector
from resources.collector_prices import CollectorAssignmentPrice
from resources.collector_requested_substitution import CollectorRequestSubstitution
from resources.collector_variety import CollectorVariety, CollectorVarietyList, CollectorVarietyListByCollector, CollectorVarietyListByProductId
from resources.collector_product import CollectorProduct, CollectorProductList
from resources.working_price import WorkingPrice
from resources.substitution_back_history import SubstitutionBackHistory
from resources.quality_assurance_assignment import CollectorQualityAssuranceAssignment, CollectorPortalQualityAssuranceAssignment
from resources.price_imputation import PriceImputation
from resources.outlier_detection import OutlierDetections , OutlierDetection


# from resources.outlet import Outlet, OutletList
from resources.collector_substitution import SubstitutionList 
from resources.cpi_variety import CPIVariety, CPIVarietyList
from werkzeug.exceptions import HTTPException
from flask_cors import CORS

from resources.time_period import CurrentTimePeriod, TimePeriods
from resources.collector_user import ChangePassword, Login, User, VerifyToken

# from data_loaders.areas import AreasRawDataLoader
# from data_loaders.outlets import OutletsRawDataLoader
# from data_loaders.varieties import VarietiesRawDataLoader
# from data_loaders.products import ProductsRawDataLoader
# from data_loaders.assignments import AssignmentsRawDataLoader

app = Flask(__name__) 
CORS(app)
api = Api(app)

bcrypt = Bcrypt(app)

app.config["JWT_SECRET_KEY"] = "super-secret" 
app.config['JWT_TOKEN_LOCATION'] = ['headers']
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=20)

jwt = JWTManager(app)


#CONFIGURES THE DATA BASE CONNECTION
app.config['SQLALCHEMY_DATABASE_URI'] = PORTAL_CONFIG_STRING
# app.config['SQLALCHEMY_BINDS'] = {'cpi': 'mysql://sib_gian:Letmein123!@192.168.0.3/cpi2' }

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
api.add_resource( CPIVariety, '/api/cpi-varieties/<string:id>')
api.add_resource( CPIVarietyList, '/api/cpi-varieties')


# ---------------------- PORTAL SUBSTITUTION ENDPOINTS --------------------

# used by the collector and headquarter to upload substitutions
api.add_resource( SubstitutionList , '/api/substitutions')


# used by the collector to request and the headquarter to approve substitution requested 
api.add_resource(CollectorRequestSubstitution, '/api/request-substitutions')


# ---------------------- PORTAL ASSIGNMENTS ENDPOINTS --------------------

# used by the headquarter to get an assignment by id
api.add_resource( CollectorAssignment, '/api/assignments/<string:id>')

# used by headquarter to get all the assignments
api.add_resource( CollectorAssignmentList , '/api/assignments')


# used by headquarter to get current assignments data
api.add_resource( CollectorFilterAssignments , '/api/current-assignments')

# used by the headquarters to get the automated assignments
api.add_resource( CollectorAutomatedAssignmentList, '/api/assignments/automated-assignments') 
api.add_resource( SyncAutomatedAssignments , '/api/assignments/sync-automated-assignments')

# used by the headquarter to get the assignments collected by the headquarter only
api.add_resource( CollectorHeadquarterAssignmentList, '/api/assignments/headquarter-assignments') 

# used by the collector to get the assignments by collector id 
api.add_resource( CollectorAssignmentListByCollector, '/api/assignments/user-assignments/<string:collector_id>') 

# used by the HQ to find all assignments substituted with a new variety
api.add_resource( CollectorAssignmentSubstitutionsWithNewVariety, '/api/assignments/new-variety-substitution-assignments') 

#used to get the stats for a dashboard 
api.add_resource( CollectorAssignmentStatistics, '/api/assignments/collection-statistics')
api.add_resource( CollectorOutletCoverageStats, '/api/assignments/outlet-coverage-statistics')

# used by the collector to upload the prices of the assignments collected. Also used by the headquarter to update a price
api.add_resource( UploadAssignmentsPrices , '/api/assignments-upload')


# ---------------------- PORTAL ASSIGNMENT PRICES ENDPOINTS --------------------

# used by the the headquarter to update the status of the assignments
api.add_resource( CollectorAssignmentPrice, '/api/assignment-prices' )

# api.add_resource( ExportAssignmentCollection, '/api/export-assignment-collection' )


# ---------------------- PORTAL VARIETIES ENDPOINTS --------------------
api.add_resource( CollectorVariety, '/api/varieties/<string:id>')
api.add_resource( CollectorVarietyList, '/api/varieties')


# used specifically by the collector to get the varieties based on the assignments to be collected
api.add_resource( CollectorVarietyListByCollector, '/api/varieties/user-varieties/<int:collector_id>') 
api.add_resource( CollectorVarietyListByProductId, '/api/varieties/product-varieties/<int:product_id>')

# ---------------------- PORTAL OUTLETS ENDPOINTS --------------------
api.add_resource( CollectorOutletList, '/api/outlets' ) 
api.add_resource( CollectorOutlet, '/api/outlets/<string:id>' ) 
api.add_resource( CollectorOutletByArea, '/api/outlets/area-outlets/<int:area_id>' ) 


# used specifically by the collector to get the outlets based on the assignments to be collected
api.add_resource( CollectorOutletListByCollector, '/api/outlets/user-outlets/<int:collector_id>') 

# ---------------------- PORTAL PRODUCTS ENDPOINTS --------------------------
api.add_resource( CollectorProductList, '/api/products')
api.add_resource( CollectorProduct, '/api/products/<string:id>')

# ---------------------- PORTAl AREA ENDPOINTS 
api.add_resource( CollectorAreaList, '/api/areas') 
api.add_resource( CollectorArea, '/api/areas/<string:id>')       


# ---------------------- GENERAL FORM DATA GETTERS ----------------------
api.add_resource( Collectors, '/api/collectors')
api.add_resource( TimePeriods, '/api/time_periods')
api.add_resource( CurrentTimePeriod, '/api/current-time-period')

# ---------------------- AUTHENTICATION ----------------------
api.add_resource( Login, '/api/login')
api.add_resource( VerifyToken, '/api/verify-token/<string:type>')
api.add_resource( ChangePassword, '/api/change-password/<int:id>')
api.add_resource( User, '/api/user')


# ---------------------- DATA LOADERS FROM SIMA ----------------------

api.add_resource( ProductsDataImport, '/api/import-products')
api.add_resource( VarietiesDataImport, '/api/import-varieties')
api.add_resource( OutletsDataImport, '/api/import-outlets')
api.add_resource( AssignmentDataImport, '/api/import-assignments')
api.add_resource( PricesDataImport, '/api/import-prices')


# ---------------------- DATA LOADERS FROM SIMA ----------------------

# api.add_resource( ProductsRawDataLoader, "/products-dataloader" )
# api.add_resource( VarietiesRawDataLoader, '/api/varieties-dataloader' )
# api.add_resource( AssignmentsRawDataLoader, '/api/assignments-dataloader' )
# api.add_resource( OutletsRawDataLoader, '/api/outlets-dataloader' )
# api.add_resource( AreasRawDataLoader, '/api/areas-dataloader' )

# ---------------------- PORTAL DATA CONFIGURATION --------------------
api.add_resource( UploadExcel, '/api/read-excel')
# api.add_resource( ConfigurePortalVarieties, '/api/configure-portal-varieties/<string:filename>' )
# api.add_resource( ConfigureAutomatedAssignments, '/api/configure-automated-assignments/<string:filename>' )
api.add_resource( ConfigureAssignments, '/api/configure-assignments/<string:filename>' )


# ---------------------- PORTAL WORKING PRICE ENDPOINTS ----------------------
api.add_resource( WorkingPrice, '/api/working-prices')
api.add_resource( SubstitutionBackHistory, '/api/substitution-back-history' )

# ---------------------- MOBILE APP QUALITY ASSURANCE ENDPOINTS ----------------------
api.add_resource( CollectorQualityAssuranceAssignment, '/api/quality-assurance-assignment')

# ---------------------- PORTAL QUALITY ASSURANCE ENDPOINTS ----------------------
api.add_resource( CollectorPortalQualityAssuranceAssignment, '/api/portal-quality-assurance-assignment')

# ---------------------- PRICE IMPUTATION PORTAL ENDPOINTS ----------------------
api.add_resource( PriceImputation, '/api/price-imputation')

# ---------------------- PRICE OUTLIERS DETECTION PORTAL ENDPOINTS ----------------------
api.add_resource( OutlierDetections, '/api/price-outlier-detection')
api.add_resource( OutlierDetection, '/api/price-outlier-detection/<int:id>')



#RUNS THE API
if __name__ == '__main__':
    app.run( host='0.0.0.0', port=8080, debug=True )


