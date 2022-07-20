from datetime import datetime
from db import db, portal_db

class CollectorVarietyModel(db.Model):

    __tablename__ = 'collector_variety'

    id = db.Column(db.Integer, primary_key=True)
    cpi_variety_id = db.Column(db.Integer, unique=True, nullable=True)
    name = db.Column(db.String(80), nullable=False)
    code = db.Column(db.String(80), nullable=False)
    approved_by = db.Column(db.Integer, nullable=True)
    date_approved = db.Column(db.DateTime, nullable=True)
    product_id = db.Column(db.Integer, db.ForeignKey('collector_product.id') ,  nullable=False)

    def __init__(self, name, code, product_id, cpi_variety_id=None, approved_by=None, date_approved=None, _id=None):
        self.id = _id
        self.cpi_variety_id = cpi_variety_id
        self.name = name
        self.code = code
        self.product_id = product_id
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
            'date_approved': str(self.date_approved) if self.date_approved else None
        }

    def save_to_db(self):
        db.session.add(self)
        db.session.commit()

    def update(self, newVariety):
        self.name = newVariety.name
        self.code = newVariety.code
        self.product_id = newVariety.product_id

    @classmethod
    def find_by_id(cls, id):
            return cls.query.filter_by(id=id).first()

    @classmethod
    def find_by_cpi_variety_id(cls, id):
            return cls.query.filter_by(cpi_variety_id=id).first()

    @classmethod
    def find_all(cls):
        return cls.query.all()


    @classmethod
    def find_by_collector(cls, collector_id):

        datetime_now = datetime.now()
        period = f"{datetime_now.year}-{datetime_now.month}-01"


        query = """
            SELECT DISTINCT 
                collector_variety.cpi_variety_id, 
                collector_variety.name, 
                collector_variety.code
            FROM collector_variety 
            WHERE collector_variety.product_id IN (
            	
                SELECT c_v.product_id
                FROM assignment
                JOIN collector_variety as c_v on c_v.cpi_variety_id = assignment.variety_id
               	WHERE assignment.collector_id = %s
            	AND assignment.time_period = %s
                AND collector_variety.cpi_variety_id IS NOT NULL
            )
        """
        portal_db.execute(query, (collector_id, period,))
        varieties = portal_db.fetchall()

        varieties = [ {
            'id': variety[0],
            'name': variety[1],
            'code': variety[2]
        } for variety in varieties ]


        

        return varieties
