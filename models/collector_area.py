from db import db

class CollectorAreaModel(db.Model):

    __tablename__ = 'collector_area'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    areaid = db.Column(db.String(255), nullable=False)
    
    # outlets = db.relationship("CollectorOutletModel", backref="area")
  
    def __init__(self, name, areaid, _id=None, outlets=None):
        self.id = _id
        self.name = name
        self.areaid = areaid
        # self.outlets = [] if not outlets else outlets

    def __str__(self):
        return str(self.json())

    def json(self): 
        return {
            "id": self.id,
            "name": self.name,
            "areaid": self.areaid,
            # "outlets": [outlet.json() for outlet in self.outlets]
        }

    def save_to_db(self):
       
        db.session.add(self)
        db.session.commit()

    def update(self, new_area):
        self.name = new_area.name
        self.areaid = new_area.areaid
        db.session.commit()

    @classmethod
    def find_by_id(cls, id):
        return cls.query.filter_by(id=id).first()

    @classmethod
    def find_all(cls):
        return cls.query.all()