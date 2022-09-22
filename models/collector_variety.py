from datetime import datetime
from re import S
from db import db, get_portal_db_connection

class CollectorVarietyModel(db.Model):

    __tablename__ = 'collector_variety'

    id = db.Column(db.Integer, primary_key=True)
    cpi_variety_id = db.Column(db.Integer, unique=True, nullable=True)
    name = db.Column(db.String(80), nullable=False)
    code = db.Column(db.String(80), nullable=False)
    approved_by = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey('user.id') ,  nullable=False)
    date_approved = db.Column(db.DateTime, nullable=True)
    product_id = db.Column(db.Integer, db.ForeignKey('collector_product.id') ,  nullable=False)

    def __init__(self, name, code, product_id, created_by, cpi_variety_id=None, approved_by=None, date_approved=None, _id=None):
        self.id = _id
        self.cpi_variety_id = cpi_variety_id
        self.name = name
        self.code = code
        self.product_id = product_id
        self.created_by = created_by
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
    def find_by_product(cls, product_id):
        return cls.query.filter_by(product_id=product_id).all()

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
                FROM assignment_with_substitution as assignment
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
