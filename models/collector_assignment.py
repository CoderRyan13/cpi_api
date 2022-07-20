from datetime import date, datetime
import json
from db import db, cpi_db
import enum
from sqlalchemy import Enum


class StatusEnum(enum.Enum):
    active = 'active'
    inactive = 'inactive'
    rejected = 'rejected'

    def __json__(self):
        return self.value

    def __str__(self):
        return self.value

class AssignmentModel(db.Model):

    __tablename__ = 'assignment'
    id = db.Column(db.Integer, primary_key=True)
    outlet_product_variety_id = db.Column(db.Integer, nullable=False)
    comment = db.Column(db.String(255), nullable=True)
    new_price = db.Column(db.Float, nullable=False)
    previous_price = db.Column(db.Float, nullable=True)
    time_period = db.Column(db.Date, nullable=False)
    outlet_id = db.Column(db.Integer, nullable=False)
    outlet_name = db.Column(db.String(255), nullable=False)
    variety_id = db.Column(db.Integer, nullable=False)
    variety_name = db.Column(db.String(255), nullable=False)
    code = db.Column(db.String(255), nullable=False)
    approved_by = db.Column(db.Integer, nullable=True)
    date_approved = db.Column(db.DateTime, nullable=True)
    collector_id = db.Column(db.Integer, nullable=False)
    collector_name = db.Column(db.String(255), nullable=False)
    last_collected = db.Column(db.DateTime, nullable=True)
    create_date_time = db.Column(db.DateTime, nullable=False)
    update_date_time = db.Column(db.DateTime, nullable=False)
    status = db.Column(Enum(StatusEnum), nullable=False)    # active, inactive, rejected

    substitution = db.relationship('SubstitutionModel', backref='assignment', uselist=False, lazy=True)

    def __init__(
    self, 
    outlet_product_variety_id, 
    previous_price, 
    outlet_id, 
    outlet_name,
    variety_id, 
    variety_name,  
    code, 
    collector_id, 
    collector_name,
    time_period: date,
    last_collected=None, _id=None, approved_by=None, date_approved=None, substitution=None, create_date_time=None, update_date_time=None):

        self.id = _id
        self.outlet_product_variety_id = outlet_product_variety_id
        self.comment = ""
        self.new_price = None
        self.previous_price = previous_price
        self.time_period = f'{time_period.year}-{time_period.month}-01'
        self.outlet_id = outlet_id
        self.outlet_name = outlet_name
        self.variety_id = variety_id
        self.variety_name = variety_name
        self.code = code
        self.approved_by = approved_by
        self.date_approved = date_approved
        self.last_collected = last_collected
        self.collector_id = collector_id
        self.collector_name = collector_name
        self.substitution = substitution
        self.create_date_time = create_date_time
        self.update_date_time = update_date_time
        self.status = StatusEnum.inactive

    def json(self):

        return {
            "id": self.id,
            "outlet_product_variety_id": self.outlet_product_variety_id,
            "comment": self.comment,
            "new_price": self.new_price,
            "previous_price": str(self.previous_price),
            "time_period": str(self.time_period),
            "outlet_id": self.outlet_id,
            "outlet_name": self.outlet_name,
            "variety_id": self.variety_id,
            "variety_name": self.variety_name,
            "code": self.code,
            "substitution": self.substitution.json() if self.substitution else None,
            "approved_by": self.approved_by,
            "date_approved": str(self.date_approved) if self.date_approved  else None,
            "collector_id": self.collector_id,
            "collector_name": self.collector_name,
            "last_collected":  str(self.last_collected) if self.last_collected else None,
            "create_date_time": str(self.create_date_time) if self.create_date_time  else None,
            "update_date_time": str(self.update_date_time) if self.update_date_time  else None,
            "status": str(self.status),
        }
    
    def __str__(self): 
        return str(self.json())

    def save_to_db(self):

        assignment = AssignmentModel.find_by_opv_id_and_time_period_and_collector_id(self.outlet_product_variety_id, self.time_period, self.collector_id)

        if assignment:
            assignment.update(self)

        else:
            db.session.add(self)
            db.session.commit()

            if self.substitution:
                self.substitution.save_to_db()
                db.session.commit()
                

    def update(self, newAssignment):

        newAssignment.id = self.id # this is the id of the old assignment
        self.comment = newAssignment.comment
        self.new_price = newAssignment.new_price
        self.previous_price = newAssignment.previous_price
        self.time_period = newAssignment.time_period
        self.outlet_id = newAssignment.outlet_id
        self.outlet_name = newAssignment.outlet_name
        self.code = newAssignment.code
        self.collector_id = newAssignment.collector_id
        self.collector_name = newAssignment.collector_name
        self.last_collected = newAssignment.last_collected
        self.update_date_time = datetime.now()

    def sync_from_cpi(self, cpi_assignment):

        if self.update_date_time < cpi_assignment.update_date_time:

            cpi_assignment.id = self.id # this is the id of the old assignment
            self.comment = cpi_assignment.comment
            self.previous_price = cpi_assignment.previous_price
            self.time_period = cpi_assignment.time_period
            self.outlet_id = cpi_assignment.outlet_id
            self.outlet_name = cpi_assignment.outlet_name
            self.code = cpi_assignment.code
            self.collector_id = cpi_assignment.collector_id
            self.collector_name = cpi_assignment.collector_name
            self.last_collected = cpi_assignment.last_collected
            db.session.commit()

    def update_status(self, status: StatusEnum):
        self.status = status
        self.update_date_time = datetime.now()
        db.session.commit()

    @classmethod
    def find_by_id(cls, id):
        return cls.query.filter_by(id=id).first()

    @classmethod
    def find_by_collector(cls, collector_id):
        datetime_now = datetime.now()
        period = f"{datetime_now.year}-{datetime_now.month}-01"
        return cls.query.filter_by(collector_id=collector_id, time_period=period).all()

    @classmethod
    def find_by_opv_id_and_time_period_and_collector_id(cls, opv_id, time_period, collector_id):
        return cls.query.filter_by(outlet_product_variety_id=opv_id, time_period=time_period, collector_id=collector_id).first()

    @classmethod
    def find_all(cls, args):
        filters = {
            "time_period": args.get("time_period", None),
            "collector_id": args.get("collector_id", None),
        }
        filters = {k: v for k, v in filters.items() if v}
        return cls.query.filter_by(**filters).all()

    @classmethod
    def find_all_by_collector_id(cls, collector_id):
        return cls.query.filter_by(collector_id=collector_id).all() 

    @classmethod
    def load_period_assignment(cls):

        #query the database

        query = """
            SELECT
            opv.id as outlet_product_variety_id,
            (
                SELECT comment 
                FROM price 
                WHERE price.outlet_vareity_id = opv.id 
                ORDER BY time_period DESC 
                LIMIT 1
            ) as comment,
            (
                SELECT price 
                FROM price 
                WHERE price.outlet_vareity_id = opv.id 
                ORDER BY time_period DESC 
                LIMIT 1
            ) as new_price,
            (
                SELECT price 
                FROM price 
                WHERE price > 0 AND price.outlet_vareity_id = opv.id 
                ORDER BY time_period DESC 
                LIMIT 1
            ) as previous_price,
            (
                SELECT CONCAT(time_period, "") as time_period 
                FROM price 
                WHERE price.outlet_vareity_id = opv.id 
                ORDER BY time_period DESC 
                LIMIT 1
            ) as time_period,
            o.id as outlet_id,
            o.est_name as outlet_name,
            v.code,
            uop.user_id as collector_id,
            (
                SELECT name 
                FROM user 
                WHERE user.id = uop.user_id 
                LIMIT 1
            ) as collector_name,
            (
                SELECT CONCAT(time_period, "") 
                FROM price 
                WHERE price > 0 AND price.outlet_vareity_id = opv.id 
                ORDER BY time_period DESC 
                LIMIT 1
            ) as date_last_collected
            FROM outlet_product_variety as opv 
            JOIN variety as v on opv.variety_id = v.id 
            JOIN outlet_product as op on opv.outlet_product_id = op.id 
            JOIN user_outlet_product as uop on uop.outlet_product_id = op.id
            JOIN outlet as o on op.outlet_id = o.id 
            WHERE opv.is_active = 1
        """

        cpi_db.execute(query)
        assignments = cpi_db.fetchall()

        #create a list of assignments
        assignment_list = [ 
            AssignmentModel(assignment[0], assignment[1], assignment[2], assignment[3], assignment[4], assignment[5], assignment[6], assignment[7], assignment[8], assignment[9])
            for assignment in assignments
        ]
        
        #save the list to the database
        for assignment in assignment_list:
            assignment.save_to_db()

        return assignment_list
   
