from db import db


class CPIVarietyModel(db.Model):
    __bind_key__ = 'cpi'
    __tablename__ = 'variety'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    code = db.Column(db.String(255), nullable=False)
    product_id = db.Column(db.Integer, nullable=False)


    def __init__(self, name, code, _id=None, product_id=None):
        self.name = name
        self.code = code
        self.product_id = product_id
        self.id = _id
       

    def json(self):
        return {
            'id': self.id,
            'name': self.name,
            'code': self.code,
            'product_id': self.product_id,
        }


    @classmethod
    def find_by_id(cls, id):
        return cls.query.filter_by(id=id).first()

    @classmethod
    def get_all(cls):
        return cls.query.all()