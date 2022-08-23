from datetime import datetime
from re import S
from db import db, portal_db

class CollectorVarietyModel(db.Model):

    __tablename__ = 'collector_variety'

    id = db.Column(db.Integer, primary_key=True)
    cpi_variety_id = db.Column(db.Integer, unique=True, nullable=True)
    name = db.Column(db.String(80), nullable=False)
    code = db.Column(db.String(80), nullable=False)
    approved_by = db.Column(db.Integer, nullable=True)
    date_approved = db.Column(db.DateTime, nullable=True)
    is_headquarter = db.Column(db.Integer, nullable=True)
    is_monthly = db.Column(db.Integer, nullable=True)
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
            'is_headquarter': self.is_headquarter,
            'is_monthly': self.is_monthly,
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

        period = datetime.now().strftime("%Y-%m-01")

        query = """
            SELECT DISTINCT 
                collector_variety.id, 
                collector_variety.name, 
                collector_variety.code
            FROM collector_variety 
            WHERE collector_variety.product_id IN (
                SELECT c_v.product_id
                FROM assignment
                JOIN collector_variety as c_v on c_v.id = assignment.variety_id 
               	WHERE assignment.collector_id = %s
            	AND assignment.time_period = %s
                AND collector_variety.id IS NOT NULL
            ) OR collector_variety.id IN (
                SELECT sub.variety_id
				FROM substitution as sub
				JOIN assignment on assignment.id = sub.assignment_id
                WHERE assignment.collector_id = %s
                AND assignment.time_period = %s
            )
        """
        portal_db.execute(query, (collector_id, period, collector_id, period,))
        varieties = portal_db.fetchall()

        varieties = [ {
            'id': variety[0],
            'name': variety[1],
            'code': variety[2]
        } for variety in varieties ]
        
        return varieties

    @classmethod
    def insert_many(cls, varieties):

        find_product_id_query = "SELECT id FROM collector_product WHERE code = %s"

        for variety in varieties:

            portal_db.execute(find_product_id_query, (variety['code'],))
            product_id = portal_db.fetchone()[0]

            new_variety = cls(
                variety['name'],
                variety['code'],
                product_id
            )

            db.session.add(new_variety)
            db.session.commit()
            variety['id'] = new_variety.id           
        
        return varieties
