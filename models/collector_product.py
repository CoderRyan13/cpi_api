from datetime import date, datetime
from email.policy import default
from db import db

class CollectorProductModel(db.Model):

    __tablename__ = 'collector_product'

    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(80), nullable=False)
    description = db.Column(db.String(80), nullable=False)
    status = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime,  nullable=False, default=datetime.now)
    updated_at = db.Column(db.DateTime,  nullable=True)
    varieties = db.relationship("CollectorVarietyModel", backref="product")
    
    def __init__(self, code, description, status, created_at=None, updated_at=None, varieties=None, _id=None):
        self.id = _id
        self.code = code
        self.description = description
        self.status = status
        self.created_at = created_at
        self.updated_at = updated_at
        self.varieties = [] if varieties is None else varieties
        
    def __str__(self):
        return str(self.json())


    def json(self):
        return {
            'id': self.id,
            'code': self.code,
            'description': self.description,
            'status': self.status,
            'created_at': str(self.created_at),
            'updated_at': str(self.updated_at) if self.updated_at else None,
            "varieties": [variety.json() for variety in self.varieties] if self.varieties else [],
        }

    def save_to_db(self):
        db.session.add(self)
        db.session.commit()

    @classmethod
    def find_by_id(cls, id):
            return cls.query.filter_by(id=id).first()

    @classmethod
    def find_by_code(cls, code):
            return cls.query.filter_by(code=code).first()

    @classmethod
    def find_all(cls, _args):
        search = _args.get('search', '')
        limit = _args.get('limit', 200)
        
        return cls.query.all()

    def update(self, new_outlet):
        self.code = new_outlet.code 
        self.description = new_outlet.description
        self.status = new_outlet.status
        self.updated_at = datetime.now()
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()