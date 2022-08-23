from datetime import date, datetime
from db import db, cpi_db, portal_db
import enum
from sqlalchemy import Enum

# This includes February, May, August, and November
QUARTERLY_MONTHS = ['2', '5', '8', '11']

# ______________________ COLLECTOR ASSIGNMENT ______________________


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
    variety_id = db.Column(db.Integer, db.ForeignKey("collector_variety.id"), nullable=False)
    variety_name = db.Column(db.String(255), nullable=False)
    code = db.Column(db.String(255), nullable=False)
    approved_by = db.Column(db.Integer, nullable=True)
    date_approved = db.Column(db.DateTime, nullable=True)
    collector_id = db.Column(db.Integer, nullable=False)
    collector_name = db.Column(db.String(255), nullable=False)
    last_collected = db.Column(db.DateTime, nullable=True)
    collected_at = db.Column(db.DateTime, nullable=True)
    can_substitute = db.Column(db.Integer, nullable=False)
    create_date_time = db.Column(db.DateTime, nullable=False)
    update_date_time = db.Column(db.DateTime, nullable=False)
    status = db.Column( db.Enum("active", "inactive", "rejected", "approved"), nullable=False) 

    substitution = db.relationship('SubstitutionModel', backref='assignment', uselist=False, lazy=True)
    variety = db.relationship('CollectorVarietyModel', backref='assignment', uselist=False, lazy=True)
    requested_substitution = db.relationship('RequestedSubstitutionModel', backref='assignment', uselist=False, lazy=True)
    

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
    last_collected: date, _id=None, approved_by=None, date_approved=None, substitution=None, create_date_time=None, update_date_time=None):

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
        self.status = 'active'

    def json(self):

        return {
            "id": self.id,
            "outlet_product_variety_id": self.outlet_product_variety_id,
            "comment": self.comment,
            "new_price": str(self.new_price) if self.new_price else None,
            "previous_price": str(self.previous_price) if self.previous_price else None,
            "time_period": str(self.time_period),
            "outlet_id": self.outlet_id,
            "outlet_name": self.outlet_name,
            "variety_id": self.variety_id,
            "variety_name": self.variety_name,
            "code": self.code,
            "substitution": self.substitution.json() if self.substitution else None,
            "requested_substitution": self.requested_substitution.json() if self.requested_substitution else None,
            "approved_by": self.approved_by,
            "date_approved": str(self.date_approved) if self.date_approved  else None,
            "collector_id": self.collector_id,
            "collector_name": self.collector_name,
            "last_collected":  str(self.last_collected) if self.last_collected else None,
            "collected_at":  str(self.collected_at) if self.collected_at else None,
            "can_substitute":  True if self.can_substitute else False,
            "create_date_time": str(self.create_date_time) if self.create_date_time  else None,
            "update_date_time": str(self.update_date_time) if self.update_date_time  else None,
            "variety": self.variety.json() if self.variety else None,
            "status": self.status,
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

    def update_status(self, status):
        self.status = status
        self.update_date_time = datetime.now()
        db.session.commit()

    def update_assignment_price(self, new_price, collected_at):
        self.status = "active"
        self.new_price = new_price
        self.collected_at = collected_at
        db.session.commit()

    @classmethod
    def find_by_id(cls, id):
        return cls.query.filter_by(id=id).first()

    @classmethod
    def find_by_collector(cls, collector_id):

        #Get the current time period
        datetime_now = datetime.now()
        period = datetime.now().strftime("%Y-%m-01")

        # verify if the current time period includes only the monthly varieties 
        is_quarterly_period = datetime_now.month in QUARTERLY_MONTHS

        assignments = []

        # if is quarterly period, get all the assignments, otherwise only the monthly
        if is_quarterly_period:
            assignments = cls.query.filter_by(collector_id=collector_id, time_period=period ).all()
            assignments = [assignment for assignment in assignments if assignment.variety.is_headquarter == 0] 

        else :
            assignments = cls.query.filter_by(collector_id=collector_id, time_period=period).all()
            assignments = [assignment for assignment in assignments if (assignment.variety.is_headquarter == 0 and assignment.variety.is_monthly == 1)] 

        # filter the ones that are not automated assignments
        assignments = [assignment for assignment in assignments if assignment.get_automated_assignment() is None]

        
        return assignments


    def get_automated_assignment(self):

        query = """SELECT 
                        id, 
                        code, 
                        outlet_id, 
                        from_outlet_id 
                    FROM automated_assignment 
                    WHERE outlet_id = %s AND code = %s"""

        portal_db.execute(query, (self.outlet_id, self.code))
        automated_assignment = portal_db.fetchone()
        return automated_assignment if automated_assignment else None


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
   
