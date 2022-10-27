from datetime import datetime
from db import db
from models.collector_variety import CollectorVarietyModel
from models.settings import SettingsModel


class CollectorPriceModel(db.Model):

    __tablename__ = 'price'
    id = db.Column(db.Integer, primary_key=True)
    assignment_id = db.Column(db.Integer, db.ForeignKey("assignment.id"), nullable=False)
    comment = db.Column(db.String(255), nullable=True)
    price = db.Column(db.Float, nullable=False)
    time_period = db.Column(db.Date, nullable=False)
    collected_at = db.Column(db.DateTime, nullable=True)
    collector_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    updated_by = db.Column(db.Integer, nullable=True)
    updated_at = db.Column(db.DateTime, nullable=True)
    status = db.Column( db.Enum("rejected", "approved"), nullable=True, default=None) 
        

    def __init__(
        self, 
        assignment_id,
        comment,
        price,
        collected_at,
        collector_id
    ):

        current_time_period = SettingsModel.get_current_time_period()
      
        self.assignment_id = assignment_id
        self.comment = comment
        self.price = price
        self.time_period = current_time_period
        self.collected_at = collected_at
        self.updated_by = None
        self.updated_at = None
        self.status = None
        self.collector_id = collector_id

    def json(self):

        return {
            "id": self.id,
            "assignment_id": self.assignment_id,
            "comment": self.comment,
            "collector_id": self.collector_id,
            "price": str(self.price) if self.price else None,
            "time_period": str(self.time_period),
            "updated_by": self.updated_by,
            "updated_at": str(self.updated_at) if self.updated_at  else None,
            "collected_at":  str(self.collected_at) if self.collected_at else None,
            "status": self.status,
        }
    
    def __str__(self): 
        return str(self.json())


    # used to find the assignment record by assignment id for the current time period
    @classmethod
    def find_by_assignment_id(cls, assignment_id):
        time_period = SettingsModel.get_current_time_period()
        return cls.query.filter_by(assignment_id=assignment_id, time_period=time_period).first()
    
    # used to update assignment price
    def update_price(self, new_price, collected_at, comment, collector_id):
        self.price = new_price
        self.collected_at = collected_at
        self.comment = comment
        self.status = None
        self.collector_id = collector_id
        db.session.commit()

    # used to update assignment status
    def update_status(self, status, user_id):
        self.updated_at = datetime.now()
        self.updated_by = user_id
        self.status = status
        db.session.commit()


    # used to delete the price record from the database
    @classmethod
    def clear_assignment_price(cls, assignment_id):
        
        time_period = SettingsModel.get_current_time_period()
        prices = cls.query.filter_by(assignment_id=assignment_id, time_period=time_period ).all()

        for price in prices:
            db.session.delete(price)
        
        db.session.commit()
    
    # used to create a price record for an assignment
    @classmethod
    def create_assignment_price(cls, price):

        new_price = cls(
            assignment_id=price["assignment_id"],
            comment=price["comment"],
            price=price["price"],
            collected_at=price["collected_at"],
            collector_id=price["collector_id"],
        )
        db.session.add(new_price)
        db.session.commit()
        return new_price

