from db import db

class SubstitutionModel(db.Model):

    __tablename__ = 'substitution'
    id = db.Column(db.Integer, primary_key=True)
    variety_id = db.Column(db.Integer, db.ForeignKey('collector_variety.id'), nullable=False)
    outlet_id = db.Column(db.Integer, db.ForeignKey('collector_outlet.id'), nullable=True)
    assignment_id = db.Column(db.Integer, db.ForeignKey('assignment.id'), nullable=False, unique=True)

    variety = db.relationship("CollectorVarietyModel", backref="substitution", uselist=False, lazy=True)
    outlet = db.relationship("CollectorOutletModel", backref="substitution", uselist=False, lazy=True)


    def __init__(self, variety_id, outlet_id, _id=None, assignment_id=None):
        self.id = _id
        self.variety_id = variety_id
        self.outlet_id = outlet_id
        self.assignment_id = assignment_id
        

    def json(self):
        return {
            "id": self.id,
            "variety_id": self.variety_id,
            "outlet_id": self.outlet_id,
            "assignment_id": self.assignment_id
        }
    
    def __str__(self):
        return str(self.json())

    def update(self, new_substitution):

        if new_substitution:
            new_substitution.assignment_id = self.assignment_id  # this is the id of the old substitution
            self.variety_id = new_substitution.variety_id
            self.outlet_id = new_substitution.outlet_id

        else:
            self.delete_from_db()

    def save_to_db(self):
        db.session.add(self)
        db.session.commit()

       
    def delete_from_db(self):
        db.session.delete(self)
        db.session.commit()

    @classmethod
    def find_by_id(cls, id):
        return cls.query.filter_by(id=id).first()

    @classmethod
    def find_by_assignment_id(cls, assignment_id):
        return cls.query.filter_by(assignment_id=assignment_id).first()

    @classmethod
    def find_all(cls):
        return cls.query.all()

