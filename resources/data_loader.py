from datetime import date, datetime
from flask_restful import Resource
from db import cpi_db
from models.collector_area import CollectorAreaModel
from models.collector_assignment import AssignmentModel
from models.collector_outlet import CollectorOutletModel
from models.collector_product import CollectorProductModel
from models.collector_variety import CollectorVarietyModel


def cpi_to_collector_product(product):

    return CollectorProductModel(
        _id=product[0],
        code=product[1],
        status=product[2] if product[2] else 0,
        description=product[3],
        created_at=product[4],
        updated_at=product[5]
    )

def cpi_to_collector_variety(variety):

    return CollectorVarietyModel(
        cpi_variety_id=variety[0],
        code=variety[1],
        name=variety[2],
        product_id=variety[3],
        approved_by=1,
        date_approved=datetime.now()
    )

def cpi_to_collector_assignment(assignment):

    return AssignmentModel(
        outlet_product_variety_id =assignment[0],
        outlet_name=assignment[1],
        outlet_id=assignment[2],
        variety_id=assignment[3],
        variety_name=assignment[4],
        previous_price=assignment[5],
        code=assignment[6],
        collector_id=assignment[7],
        collector_name=assignment[8],
        last_collected=assignment[9],
        create_date_time=datetime.now(),
        update_date_time=datetime.now(),
        time_period=date.today()
    )        

def cpi_to_collector_area(area):
    
        return CollectorAreaModel(
            _id=area[0],
            name=area[1],
            areaid=area[2]
        )

class ProductsDataLoader(Resource):
    
    def get(self):
        
        #get cpi products
        cpi_db.execute("SELECT id, code, status, description, create_date_time, update_date_time FROM coicop")
        cpi_products = cpi_db.fetchall()

        #results
        synced_products = {
            "updated_products" : [],
            "new_products" : [],
        }

        #sync products to the collector db
        for product in cpi_products:
            
            cpi_product = cpi_to_collector_product(product)
            collector_product = CollectorProductModel.find_by_id(product[0])

            if collector_product:
                collector_product.update(cpi_product)
                synced_products["updated_products"].append(collector_product.json())

            else:
                cpi_product.save_to_db()
                synced_products["new_products"].append(cpi_product.json())
            
        return synced_products

class VarietiesDataLoader(Resource):

    def get(self):

        #get cpi varieties
        cpi_db.execute("SELECT id, code, name, product_id FROM variety")
        cpi_varieties = cpi_db.fetchall()

        #results
        synced_varieties = {
            "updated_varieties" : [],
            "new_varieties" : [],
        }

        #sync varieties to the collector db
        for variety in cpi_varieties:
            
            cpi_variety = cpi_to_collector_variety(variety)
            collector_variety = CollectorVarietyModel.find_by_cpi_variety_id(variety[0])

            if collector_variety:
                collector_variety.update(cpi_variety)
                synced_varieties["updated_varieties"].append(collector_variety.json())

            else:
                cpi_variety.save_to_db()
                synced_varieties["new_varieties"].append(cpi_variety.json())
            
        return synced_varieties

class OutletsDataLoader(Resource):

    def get(self):

        query = """ SELECT 
                id, 
                est_name, 
                note, 
                address, 
                lat, 
                outlet.long, 
                phone, 
                area_id 
            FROM outlet """

        #get cpi varieties
        cpi_db.execute(query)
        cpi_outlets = cpi_db.fetchall()


        #results
        synced_outlets = {
            "updated_outlets" : [],
            "new_outlets" : [],
        }

        #sync outlets to the collector db
        for outlet in cpi_outlets:
            
            cpi_outlet = CollectorOutletModel.cpi_to_collector_outlet(outlet)
            collector_outlet = CollectorOutletModel.find_by_cpi_outlet_id(outlet[0])

            if collector_outlet:
                collector_outlet.update(cpi_outlet)
                synced_outlets["updated_outlets"].append(collector_outlet.json())

            else:
                print(type(cpi_outlet))
                cpi_outlet.save_to_db()
                synced_outlets["new_outlets"].append(cpi_outlet.json())
            
        return synced_outlets

class AreasDataLoader(Resource):

    def get(self):

        query = """ SELECT id, area_name, areaid FROM area"""

        #get cpi varieties
        cpi_db.execute(query)
        cpi_areas = cpi_db.fetchall()


        #results
        synced_areas = {
            "updated_areas" : [],
            "new_areas" : [],
        }

        #sync areas to the collector db
        for area in cpi_areas:
            
            cpi_area = cpi_to_collector_area(area)
            collector_area = CollectorAreaModel.find_by_id(area[0])

            if collector_area:
                collector_area.update(cpi_area)
                synced_areas["updated_areas"].append(collector_area.json())

            else:
                cpi_area.save_to_db()
                synced_areas["new_areas"].append(cpi_area.json())
            
        return synced_areas

class AssignmentDataLoader(Resource):

    def get(self):

        query = """ SELECT 
                outlet_product_variety_id, 
                outlet_name, 
                outlet_id,
                variety_id,
                variety_name, 
                previous_price, 
                code, 
                collector_id, 
                collector_name,
                last_collected
            FROM Assignment_View """

        #get cpi assignments
        cpi_db.execute(query)
        cpi_assignments = cpi_db.fetchall()


        #results
        synced_assignments = {
            "new_assignments" : [],
        }

        #sync varieties to the collector db
        for assignment in cpi_assignments:
            
            cpi_assignment = cpi_to_collector_assignment(assignment)
            collector_assignment = AssignmentModel.find_by_opv_id_and_time_period_and_collector_id( cpi_assignment.outlet_product_variety_id, cpi_assignment.time_period, cpi_assignment.collector_id) 

            if not collector_assignment:
                cpi_assignment.save_to_db()
                synced_assignments["new_assignments"].append(cpi_assignment.json())

        return synced_assignments




