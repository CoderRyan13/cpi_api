from db import db
from models.collector_assignment import AssignmentModel

class SubstitutionModel(db.Model):

    __tablename__ = 'substitution'
    id = db.Column(db.Integer, primary_key=True)
    variety_id = db.Column(db.Integer, db.ForeignKey('collector_variety.id'), nullable=False)
    outlet_id = db.Column(db.Integer, db.ForeignKey('collector_outlet.id'), nullable=True)
    price = db.Column(db.Float, nullable=False)
    assignment_id = db.Column(db.Integer, db.ForeignKey('assignment.id'), nullable=False, unique=True)

    variety = db.relationship("CollectorVarietyModel", backref="substitution", uselist=False, lazy=True)
    outlet = db.relationship("CollectorOutletModel", backref="substitution", uselist=False, lazy=True)

    def __init__(self, variety_id, outlet_id, price, _id=None, assignment_id=None):
        self.id = _id
        self.variety_id = variety_id
        self.outlet_id = outlet_id
        self.price = price
        self.assignment_id = assignment_id
        

    def json(self):
        return {
            "id": self.id,
            "variety_id": self.variety_id,
            "outlet_id": self.outlet_id,
            "assignment_id": self.assignment_id,
            "price": str(self.price)
        }
    
    def __str__(self):
        return str(self.json())

    def update(self, new_substitution):
        self.variety_id = new_substitution["variety_id"]
        self.outlet_id = new_substitution["outlet_id"]
        self.price = new_substitution["price"]
        db.session.commit()

    @classmethod
    def save_to_db(cls, substitution):


        # verify if a substitution already exist for this assignment_id
        existing_substitution = cls.find_by_assignment_id(substitution["assignment_id"])

        if existing_substitution:
            existing_substitution.update(substitution)
             # find assignment to update price and collected at 
            assignment: AssignmentModel = AssignmentModel.find_by_id(substitution["assignment_id"])
            assignment.update_assignment_price(existing_substitution.price, substitution["collected_at"]) 
            return existing_substitution

        else:
            
            # otherwise create a new substitution
            new_substitution = SubstitutionModel(
                assignment_id= substitution["assignment_id"],
                outlet_id= substitution["outlet_id"], 
                price= substitution["price"],
                variety_id=substitution["variety_id"],
            )

            # add the substitution to the database
            db.session.add(new_substitution)
            db.session.commit()

             # find assignment to update price and collected at 
            assignment: AssignmentModel = AssignmentModel.find_by_id(substitution["assignment_id"])
            assignment.update_assignment_price(new_substitution.price, substitution["collected_at"]) 
            return new_substitution
       

       
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

