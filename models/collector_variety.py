from datetime import datetime
from re import S
from db import db, get_portal_db_connection
import models.collector_product  as CP
from models.settings import SettingsModel
from sqlalchemy import desc, func
from string import ascii_lowercase
import itertools

class CollectorVarietyModel(db.Model):

    __tablename__ = 'collector_variety'

    id = db.Column(db.Integer, primary_key=True)
    cpi_variety_id = db.Column(db.Integer, unique=True, nullable=True)
    name = db.Column(db.String(80), nullable=False)
    code = db.Column(db.String(80), nullable=False)
    approved_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey('user.id') ,  nullable=False)
    date_approved = db.Column(db.DateTime, nullable=True)
    product_id = db.Column(db.Integer, db.ForeignKey('collector_product.id') ,  nullable=False)

    approved_by_user =  db.relationship("CollectorUserModel", foreign_keys=[approved_by])
    created_by_user =  db.relationship("CollectorUserModel", foreign_keys=[created_by])

    def __init__(self, name, code, product_id, created_by, cpi_variety_id=None, approved_by=None, date_approved=None, _id=None):
        self.id = _id
        self.cpi_variety_id = cpi_variety_id
        self.name = name
        self.code = code
        self.product_id = product_id
        self.created_by = created_by
        self.created_at = datetime.now()
        self.approved_by = approved_by
        self.date_approved = date_approved

    def json(self):
        return {
            'id': self.id,
            'cpi_variety_id': self.cpi_variety_id,
            'name': self.name,
            'code': self.code,
            'product_id': self.product_id,
            'approved_by': self.approved_by,
            'created_by': self.created_by,
            'date_approved': str(self.date_approved) if self.date_approved else None,
            'created_at': str(self.created_at) if self.created_at else None,
            'created_by_user': self.created_by_user.json() if self.created_by_user else None,
            'approved_by_user': self.approved_by_user.json() if self.approved_by_user else None,
        }


    def save_to_db(self):
        db.session.add(self)
        db.session.commit()

    def update(self, newVariety):
        
        # find the product by code
        product = CP.CollectorProductModel.find_by_code(newVariety['code'])

        if product:

            # update the general
            self.name = newVariety['name']
            
            # update the product if it is different
            if self.product_id != product.id:

                # Generate the new code if approved
                if self.is_approved():
                    self.code = product.generate_new_code()
                
                #otherwise just set the product code
                else:
                    self.code = product.code
                
                # update the product id
                self.product_id = product.id
                
                            
            db.session.commit()

    # checks if the variety is approved
    def is_approved(self):
        return self.approved_by is not None

    # generates a new code for the product 
    

    @classmethod
    def find_by_id(cls, id):
            return cls.query.filter_by(id=id).first()

    @classmethod
    def find_by_cpi_variety_id(cls, id):
            return cls.query.filter_by(cpi_variety_id=id).first()

    @classmethod
    def find_all(cls, filter):
        
        data_query =  cls.query.filter(func.concat(cls.name, cls.code).like(f"%{filter['search']}%"))

        total_records = data_query.count()

        if filter['sort_by'] in [ "id", "cpi_variety_id", "name", "code", "approved_by", "created_at", "created_by", "date_approved", "product_id"]:

            if filter['sort_desc'] == "true":
                data_query = data_query.order_by(getattr(cls, filter['sort_by']).desc())
            else:
                data_query = data_query.order_by(getattr(cls, filter['sort_by']).asc())
        
        if filter['page'] and filter['rows_per_page']:
            if int(filter['rows_per_page']) > 0 :
                data = data_query.paginate(int(filter['page']), int(filter['rows_per_page']), False).items
            else :
                data = data_query.all()

        else:
            data = data_query.all()

        return {"varieties": data, "count": total_records}



    @classmethod
    def find_by_product(cls, product_id):
        return cls.query.filter_by(product_id=product_id).all()


    @classmethod
    def find_by_collector(cls, collector_id):

        period = SettingsModel.get_current_time_period()

        query = """
            SELECT DISTINCT 
                collector_variety.id, 
                collector_variety.name, 
                collector_variety.code
            FROM collector_variety 
            WHERE collector_variety.product_id IN (
                SELECT c_v.product_id
                FROM current_time_period_assignments as assignment
                JOIN collector_variety as c_v on( c_v.id = assignment.variety_id OR c_v.id = assignment.substitution_variety_id ) 
               	WHERE assignment.collector_id = %s
            	AND assignment.time_period = %s
            )
        """

        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        portal_db_cursor.execute(query, (collector_id, period))
        varieties = portal_db_cursor.fetchall()
        portal_db.close()

        varieties = [ {
            'id': variety[0],
            'name': variety[1],
            'code': variety[2]
        } for variety in varieties ]
        
        return varieties

    @classmethod
    def insert_many(cls, varieties, user_id):

        find_product_id_query = "SELECT id FROM collector_product WHERE code = %s"

        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        for variety in varieties:
         
            portal_db_cursor.execute(find_product_id_query, (variety['code'],))
            product_id = portal_db_cursor.fetchone()[0]

            new_variety = cls(
                variety['name'].upper(),
                variety['code'],
                product_id,
                user_id
            )

            db.session.add(new_variety)
            db.session.commit()
            variety['id'] = new_variety.id     

        portal_db.close()
        return varieties

    @classmethod
    def find_for_substitution(cls, assignment):
            query = """
                SELECT name
                FROM collector_variety
                WHERE variety_id not in (
                    SELECT variety_id
                )
            """

    @classmethod
    def approve_variety(cls, id, user_id):

        # find the variety by id
        variety = cls.find_by_id(id)

        if variety :

            # If the variety code is greater than 23 characters means it has been approved 
            if len(variety.code) > 23:
                variety.approved_by = user_id
                variety.date_approved = datetime.now()
                db.session.commit()
                return

            # get the product by id to get a new code for variety
            product = CP.CollectorProductModel.find_by_id(variety.product_id)


            # check if any was return 
            if product:

                # generate a new code for the variety
                new_code = product.generate_new_code()

                variety.code = new_code
                variety.approved_by = user_id
                variety.date_approved = datetime.now()
                db.session.commit()
