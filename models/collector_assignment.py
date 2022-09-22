from cgi import print_exception
from datetime import date, datetime
from db import  db, get_cpi_db_connection, get_portal_db_connection
from sqlalchemy import desc
import mysql.connector
from models.collector_price import CollectorPriceModel

# This includes February, May, August, and November
QUARTERLY_MONTHS = [2, 5, 8, 11]

# Used to verify if the current month is quarterly
def is_quarterly_month():
    #Get the current time period
    datetime_now = datetime.now()

    # verify if the current time period includes only the monthly varieties 
    return datetime_now.month in QUARTERLY_MONTHS


# MAIN QUERY USED FOR THE ASSIGNMENTS 
# NOTE: THIS QUERY Can't be changed append values only
ASSIGNMENT_VIEW_QUERY = """
                SELECT 
                    id,
                    outlet_product_variety_id,
                    time_period,
                    variety_name,
                    variety_id,
                    last_collected,
                    previous_price,
                    new_price,
                    collected_at,
                    comment,
                    code,
                    outlet_name,
                    outlet_id,
                    status,
                    request_substitution_status,
                    substitution_assignment_id,
                    substitution_outlet_id,
                    substitution_variety_id,
                    substitution_price,
                    months_missing,
                    substitution_status,
                    substitution_comment,
                    product_id,
                    area_id,
                    from_outlet_id
                FROM assignment_with_substitution"""

# ______________________ COLLECTOR ASSIGNMENT ______________________


class AssignmentModel(db.Model):

    __tablename__ = 'assignment'
    id = db.Column(db.Integer, primary_key=True)
    parent_id = db.Column(db.Integer,  db.ForeignKey("assignment.id"), nullable=True)
    outlet_product_variety_id = db.Column(db.Integer, nullable=True)
    variety_id = db.Column(db.Integer, db.ForeignKey("collector_variety.id"), nullable=False)
    outlet_id = db.Column(db.Integer, db.ForeignKey("collector_outlet.id"), nullable=False)
    collector_id = db.Column(db.Integer, nullable=False)
    is_monthly = db.Column(db.Integer, nullable=False)
    is_headquarter = db.Column(db.Integer, nullable=False)
    from_outlet_id = db.Column(db.Integer, nullable=False)
    status = db.Column( db.Enum("active", "inactive"), nullable=False) 
    create_date_time = db.Column(db.DateTime, nullable=False)
    update_date_time = db.Column(db.DateTime, nullable=False)

    outlet = db.relationship("CollectorOutletModel", backref="assignments", lazy=True)
    variety = db.relationship("CollectorVarietyModel", backref="assignments", lazy=True)

    substitution  = db.relationship("AssignmentModel", lazy=True, uselist=False)
    price = db.relationship("CollectorPriceModel", primaryjoin='and_(AssignmentModel.id == CollectorPriceModel.assignment_id, CollectorPriceModel.time_period == func.date_format(func.current_date(), "%Y-%m-01") )', lazy=True, uselist=False)
    previous_price = db.relationship("CollectorPriceModel", primaryjoin='and_(AssignmentModel.id == CollectorPriceModel.assignment_id, CollectorPriceModel.time_period < func.date_format(func.current_date(), "%Y-%m-01") )', lazy='dynamic' )

    def __init__(
    self, 
    outlet_product_variety_id, 
    outlet_id, 
    variety_id, 
    collector_id, 
    parent_id,
    create_date_time=None, 
    update_date_time=None,
    status = "inactive"):

        self.outlet_product_variety_id = outlet_product_variety_id
        self.outlet_id = outlet_id
        self.variety_id = variety_id
        self.collector_id = collector_id
        self.create_date_time = create_date_time
        self.update_date_time = update_date_time
        self.parent_id = parent_id
        self.status = status

    def json(self):

        print(self.substitution)

        previous_price = self.previous_price.filter(CollectorPriceModel.price != None).order_by(desc(CollectorPriceModel.time_period)).first()

        return {
            "id": self.id,
            "outlet_product_variety_id": self.outlet_product_variety_id,
            "outlet_id": self.outlet_id,
            "variety_id": self.variety_id,
            "collector_id": self.collector_id,
            "create_date_time": str(self.create_date_time) if self.create_date_time  else None,
            "update_date_time": str(self.update_date_time) if self.update_date_time  else None,
            "status": self.status,
            'outlet': self.outlet.json(),
            'variety': self.variety.json(),
            'substitution': self.substitution.json() if self.substitution else None,
            'price': self.price.json() if self.price else None,
            "previous_price": previous_price.json() if previous_price else None 

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
    def find_by_opv_id_and_time_period_and_collector_id(cls, opv_id, time_period, collector_id):
        return cls.query.filter_by(outlet_product_variety_id=opv_id, time_period=time_period, collector_id=collector_id).first()

    # Used to get the assignments from the SIMA DB
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

        cpi_db = get_cpi_db_connection()
        cpi_db_cursor = cpi_db.cursor()
        cpi_db_cursor.execute(query)
        assignments = cpi_db_cursor.fetchall()
        cpi_db.close()

        #create a list of assignments
        assignment_list = [ 
            AssignmentModel(assignment[0], assignment[1], assignment[2], assignment[3], assignment[4], assignment[5], assignment[6], assignment[7], assignment[8], assignment[9])
            for assignment in assignments
        ]
        
        #save the list to the database
        for assignment in assignment_list:
            assignment.save_to_db()

        return assignment_list  

    # Delete all the substitution Records For the Assignment with given Id
    @classmethod
    def clear_assignment_substitution(cls, assignment_id):
        assignments = cls.query.filter_by(parent_id=assignment_id).all()
        for assignment in assignments:
            CollectorPriceModel.clear_assignment_price(assignment.id)
            db.session.delete(assignment)
        db.session.commit()

    @classmethod
    def find_assignment_substitution(cls, assignment_id):
        return cls.query.filter_by(parent_id=assignment_id).first()

    # Used to create a substitution record and save it to the database
    @classmethod
    def save_substitution(cls, substitution, collector_id):

        # Find the assignment with the given assignment_id
        assignment = cls.find_by_id(substitution['assignment_id'])

        # Verify if a substitution record exists otherwise create a new record
        assignment_sub = cls.find_assignment_substitution(substitution['assignment_id'])

        substitution_price = None

        #look for the price of the substitution

        if assignment_sub:
            substitution_price = CollectorPriceModel.find_by_assignment_id(assignment_sub.id)

            # if the substitution price has been approved then do not create a new record
            if substitution_price:
                if substitution_price.status == "approved":
                    return assignment_sub

            assignment_sub.outlet_id = substitution['outlet_id']
            assignment_sub.variety_id = substitution['variety_id']
            assignment_sub.collector = assignment.collector_id

        else:
            assignment_sub = cls(
                outlet_product_variety_id = None,
                parent_id= substitution['assignment_id'],
                outlet_id = substitution['outlet_id'],
                variety_id = substitution['variety_id'],
                collector_id = assignment.collector_id,
                create_date_time= datetime.now()
            )

            db.session.add(assignment_sub)
            
        db.session.commit()

        # clear the assignment price
        CollectorPriceModel.clear_assignment_price(substitution['assignment_id'])

        if not substitution_price:

            # Create a new price record for the substitution
            CollectorPriceModel.create_assignment_price({
                'assignment_id': assignment_sub.id,
                'price': substitution['price'],
                'comment': substitution['comment'],
                'collected_at': substitution['collected_at'],
                'collector_id': collector_id,
            })

        else: 

            # Update the price record for the substitution
            substitution_price.update_price(substitution['price'], substitution['collected_at'], substitution['comment'], collector_id)

        return assignment_sub

    # Used to verify if an assignment exist for the given outlet and variety ids
    @classmethod
    def find_assignment_by_outlet_and_variety(cls, outlet_id, variety_id):
        # get most recent assignment for the given outlet and variety order by ids if any
        assignment = cls.query.filter_by(outlet_id=outlet_id, variety_id=variety_id, status="active").order_by(desc(cls.id)).first()
        return assignment if assignment else None

    # Used to verify if a substitution exist for the given outlet and variety ids (Current Period)
    @classmethod
    def find_substitution_by_outlet_and_variety(cls, outlet_id, variety_id):
        time_period = datetime.today().strftime('%Y-%m-01')
        # get most recent substitution for the given outlet and variety order by ids if any
        substitution = cls.query.filter(cls.outlet_id == outlet_id, cls.variety_id == variety_id, cls.status =="inactive", cls.create_date_time >= time_period).order_by(desc(cls.id)).first()
        return substitution if substitution else None

    # ___________________ FETCHING ASSIGNMENTS______________________

    # Used to get the assignments collected by headquarters
    @classmethod
    def find_headquarter_assignments(cls):

        # if is quarterly period, get all the assignments, otherwise only the monthly
        assignment_query = f"""
                {ASSIGNMENT_VIEW_QUERY}
                WHERE assignment_status = 'active'
                    AND from_outlet_id IS NULL
                    AND is_headquarter = 1
                    {'AND is_monthly = 1' if is_quarterly_month() == False else '' }            
            """
        return cls.get_assignments_from_DB(assignment_query, ())

    # Used to get the assignments collected that are automated
    @classmethod
    def find_automated_assignments(cls):

        # if is quarterly period, get all the assignments, otherwise only the monthly
        assignment_query = f"""
                {ASSIGNMENT_VIEW_QUERY}
                WHERE assignment_status = 'active'
                    AND from_outlet_id IS NOT NULL
                    {'AND is_monthly = 1' if is_quarterly_month() == False else '' }            
            """
        return cls.get_assignments_from_DB(assignment_query, ())

    # Used to get the assignments collected by the given collector
    @classmethod
    def find_by_collector(cls, collector_id):

        # if is quarterly period, get all the assignments, otherwise only the monthly
        assignment_query = f"""
               {ASSIGNMENT_VIEW_QUERY}
                WHERE collector_id = %s
                AND is_headquarter = 0
                AND from_outlet_id is null 
                AND assignment_status = 'active'
                { 'AND is_monthly = 1' if is_quarterly_month() == False else '' }
            """

        return cls.get_assignments_from_DB(assignment_query, (collector_id,))

    # This method gets all the assignments from the database as 
    # queried by the query and values supplied
    @classmethod
    def get_assignments_from_DB(cls, assignment_query, values):

        portal_db_conn = get_portal_db_connection()

        db_cursor = portal_db_conn.cursor()

        db_cursor.execute(assignment_query, values)
        assignments = db_cursor.fetchall()

        portal_db_conn.close()

        new_assignments = []
        
        for assignment in assignments:

            new_assignment = {
                "id": assignment[0],
                "outlet_product_variety_id": assignment[1],
                "time_period": str(assignment[2]) if assignment[2] else None,
                "variety_name": assignment[3],
                "variety_id": assignment[4],
                "last_collected": str(assignment[5]) if assignment[5] else None,
                "previous_price": str(assignment[6]) if assignment[6] else None,
                "new_price": str(assignment[7]) if assignment[7] else None,
                "collected_at": str(assignment[8]) if assignment[8] else None,
                "comment": assignment[9],
                "code": assignment[10],
                "outlet_name": assignment[11],
                "outlet_id": assignment[12],
                "status": assignment[13],
                "request_substitution_status" : assignment[14],
                "substitution" : assignment[15],
                "can_substitute": False if assignment[19] == None else True if assignment[19] > 3 else False,
                "product_id": assignment[22],
                "area_id": assignment[23],
                "from_outlet_id": assignment[24]
            }


            # Checks if there is a substitution
            if new_assignment['substitution']:

                new_assignment['substitution'] = {
                    "id": new_assignment['substitution'],
                    "outlet_id": assignment[16],
                    "variety_id": assignment[17],
                    "price": str(assignment[18]),
                    'status': assignment[20],
                    "comment": assignment[21],
                }
                
                # GET THE PRICE & COMMENT OF THE SUBSTITUTION FOR ASSIGNMENT
                new_assignment['status'] =  assignment[20]
                new_assignment['comment'] =  assignment[21]
            
            new_assignments.append(new_assignment)
        
        return new_assignments

  

    # used to sync automated assignments
    # @classmethod
    # def sync_automated_assignments(cls):

    #     # find all automated assignments
    #     automated_assignments = cls.find_automated_assignments()

    #     # loop through the automated assignments
    #     for automated_assignment in automated_assignments:

            # find which assignment this automated assignment depends on
            # automated_assignment['from_outlet_id']