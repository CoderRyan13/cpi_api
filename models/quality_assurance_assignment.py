from db import db, get_portal_db_connection
from models.settings import SettingsModel
import datetime


# USED FOR SORTING
main_columns = [
'id',
'variety_id',
'outlet_id',
'time_period',
'variety_name',
'code',
'outlet_name',
'collector_price',
'collector_comment',
'hq_price',
'hq_comment',
'collected_at',
'substitution_outlet_name',
'substitution_variety_name',
'area_id',
'collector_collected_at',
]


class QualityAssuranceAssignmentModel(db.Model):

    __tablename__ = 'quality_assurance_assignment'
    id = db.Column(db.Integer, primary_key=True)
    assignment_id = db.Column(db.Integer)
    hq_id = db.Column(db.Integer)
    time_period = db.Column(db.Date, nullable=False)
    collector_price = db.Column(db.Float, nullable=False)
    hq_price = db.Column(db.Float, nullable=False)
    collector_comment = db.Column(db.String(255), nullable=True)
    hq_comment = db.Column(db.String(255), nullable=True)

    
    def __init__(self, assignment_id, hq_id, time_period, collector_price, hq_price, collector_comment, hq_comment, _id=None):
        self.id = _id
        self.assignment_id = assignment_id
        self.hq_id = hq_id
        self.time_period = time_period
        self.collector_price = collector_price
        self.hq_price = hq_price
        self.collector_comment = collector_comment
        self.hq_comment = hq_comment

    def __str__(self):
        return str(self.json())

    def json(self): 
        return {
            'id': self.id,
            'assignment_id': self.assignment_id,
            'hq_id': self.hq_id,
            'time_period': str(self.time_period),
            'collector_price': str(self.collector_price) if self.collector_price else None,
            'hq_price': str(self.hq_price) if self.hq_price else None,
            'collector_comment': self.collector_comment,
            'hq_comment': self.hq_comment,
        }


    @classmethod
    def generateAssuranceAssignments(cls, area_id, hq_id):

        # Get the current time period
        current_time_period = SettingsModel.get_current_time_period()


        verify_query = """
            SELECT 
                *
            FROM quality_assurance_assignment
            JOIN assignment on assignment.id = quality_assurance_assignment.id
            WHERE assignment.area_id = %s
            AND quality_assurance_assignment.hq_id = %s
            AND quality_assurance_assignment.time_period = %s
            AND quality_assurance_assignment.status = 'active'
        """

        
        conn = get_portal_db_connection()
        cursor = conn.cursor()

        cursor.execute(verify_query, (area_id, hq_id, current_time_period))
        assignments_activated = cursor.fetchall()

        if len(assignments_activated) > 0:
            return {
                "error": True,
                "message" : "These Assignments have been activated already!"
            }



        # Clean the current assignments that are existent in the db for this criteria
        clean_query = """
            DELETE FROM quality_assurance_assignment 
            WHERE  assignment_id in (SELECT assignment_id FROM assignment WHERE area_id = %s) 
            AND hq_id = %s
            AND time_period = %s
        """

        cursor.execute( clean_query, (area_id, hq_id, current_time_period))


        # get the total number of assignments completed with a price for this area
        sample_size_query = f"""
            SELECT count(*) 
            FROM price 
            WHERE time_period = '{current_time_period}' 
            AND price > 0 
            AND assignment_id in (
                SELECT assignment_id 
                FROM assignment as ass
                WHERE ass.area_id = %s 
                AND ass.from_assignment_id is null
                AND ass.is_headquarter is null
            )  """


        cursor.execute(sample_size_query, (area_id,))
        total_assignments_collected = cursor.fetchone()[0]
        print(total_assignments_collected)

        sample_size = round(total_assignments_collected * .1)

        print("Sample size" , sample_size)

        # First We need to randomly select assignments that were collected or are substitutions
        main_query = f"""

            SELECT 

                ass.id as id,
                price.price as price,
                price.comment as collector_comment,
                sub_p.price as substitution_price,
                sub_variety.name as substitution_variety,
                sub_outlet.est_name as substitution_outlet,

                sub_p.collected_at as substitution_price_collected_at,
                price.collected_at as collected_at,
                sub_p.comment as substitution_price_comment
                

            FROM `assignment` as ass
            LEFT JOIN price on ass.id = price.assignment_id AND price.time_period = '{current_time_period}'
            LEFT JOIN assignment as substitution on substitution.parent_id  = ass.id AND DATE_FORMAT(substitution.create_date_time, '%Y-%m-01') = DATE_FORMAT( '{current_time_period}' , '%Y-%m-01')
            LEFT JOIN collector_variety as sub_variety ON sub_variety.id = substitution.variety_id
            LEFT JOIN collector_outlet as sub_outlet ON sub_outlet.id = substitution.outlet_id
            LEFT JOIN price as sub_p on substitution.id = sub_p.assignment_id AND sub_p.time_period = DATE_FORMAT( '{current_time_period}' , '%Y-%m-01')
            WHERE (price.price > 0 OR sub_p.price > 0) 
            AND ass.status = 'active'
            AND ass.area_id = %s
            AND ass.from_assignment_id is null
            AND ass.is_headquarter is null
            ORDER BY RAND()
            LIMIT %s

        """

        cursor.execute(main_query, (area_id, sample_size))
        assignments = cursor.fetchall()

        insertQuery = """
            INSERT INTO quality_assurance_assignment (
                assignment_id, 
                collector_price, 
                collector_comment, 
                time_period, 
                hq_id,
                substitution_variety,
                substitution_outlet,
                collector_collected_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
        """

        accuracy_assurance_assignments = [
            (
                ass[0], 
                ass[1] if ass[1] else ass[3], 
                ass[8] if ass[4] else ass[2], #if there is a substitution variety just use sub comment else price comment
                current_time_period,
                hq_id,
                ass[4],
                ass[5],
                ass[8] if ass[6] else ass[7], #if there is a substitution variety just use sub collector_collected_at else price collector_collected_at

            )
            for ass in assignments
        ]

        cursor.executemany(insertQuery, accuracy_assurance_assignments)
        conn.commit()
        conn.close()

        return {
                "success": True,
                "added": sample_size
            }


    @classmethod
    def getAssuranceAssignments(cls, filter):

        area_filter = ""
        hq_filter = ""
        time_period_filter = ""
        values = []


        print(filter)

        if filter.get('area_id', None):
            values.append(filter['area_id'])
            area_filter = "AND ass.area_id = %s"

        if filter.get('hq_id', None):
            values.append(filter['hq_id'])
            hq_filter = "AND q_assurance_ass.hq_id = %s"

        if filter.get('time_period', None):
            values.append(filter['time_period'])
            time_period_filter = "AND q_assurance_ass.time_period = %s"
        
        query = f"""
             SELECT 

                ass.id as id,
                ass.variety_id as variety_id,
                ass.outlet_id as outlet_id,
                q_assurance_ass.time_period as time_period,
                variety.name as variety_name,
                variety.code as code,
                outlet.est_name as outlet_name,
                q_assurance_ass.collector_price as collector_price,
                q_assurance_ass.collector_comment as collector_comment,
                q_assurance_ass.hq_price as hq_price,
                q_assurance_ass.hq_comment as hq_comment,
                q_assurance_ass.collected_at as collected_at,
                q_assurance_ass.substitution_outlet as substitution_outlet_name,
                q_assurance_ass.substitution_variety as substitution_variety_name,
                ass.area_id as area_id,
                q_assurance_ass.collector_collected_at as collector_collected_at


            FROM quality_assurance_assignment as q_assurance_ass
            JOIN assignment as ass ON q_assurance_ass.assignment_id = ass.id
            JOIN collector_variety as variety ON ass.variety_id = variety.id
            JOIN collector_outlet as outlet ON ass.outlet_id = outlet.id
            WHERE q_assurance_ass.status = 'active'
            {area_filter}
            {hq_filter}
            {time_period_filter}
        """

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, tuple(values))
        assignments = cursor.fetchall()

        assignments = [
            {
                "id": ass[0],
                "variety_id": ass[1],
                "outlet_id": ass[2],
                "time_period": str(ass[3]),
                "variety_name": ass[4],
                "code": ass[5],
                "outlet_name": ass[6],
                "previous_price": str(ass[7]) if ass[7] else None,
                "collector_comment": ass[8],
                "new_price": str(ass[9]) if ass[9] else None,
                "comment": ass[10],
                "collected_at" : str(ass[11]) if ass[11] else None,
                "last_collected" : str(datetime.date.today()),
                "status": 'approved' if ass[9] else 'pending',
                "request_substitution_status" : None,
                "can_substitute": False,
                "substitution": None,
                "substitution_outlet_name": ass[12],
                "substitution_variety_name": ass[13],
                "collector_collected_at": str(ass[15]) if ass[15] else None
            } for ass in assignments
        ]


        print(assignments)


        return assignments

    @classmethod
    def updateAssignmentPrice(cls, assignments):

        # get current time-period
        current_period = SettingsModel.get_current_time_period()

        # prepare assignments
        assignment_data = [
            (
                assignment["new_price"],
                assignment["comment"],
                assignment["collected_at"],
                assignment["id"],
                current_period
            ) for assignment in assignments
        ]

        query = """
            UPDATE quality_assurance_assignment SET
            hq_price = %s,
            hq_comment = %s,
            collected_at = %s
            WHERE assignment_id = %s AND time_period = %s
        """

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.executemany(query, assignment_data)
        conn.commit()
        conn.close()

        return { "success": True, "saved": len(assignment_data)  }



    @classmethod
    def getPortalAssuranceAssignments(cls, filter):

        base_query = f""" 
            SELECT 
                id,
                variety_id,
                outlet_id,
                time_period,
                variety_name,
                code,
                outlet_name,
                collector_price,
                collector_comment,
                hq_price,
                hq_comment,
                collected_at,
                substitution_outlet_name,
                substitution_variety_name,
                area_id,
                collector_collected_at,
                hq_id,
                status
                
            FROM assurance_assignments_view
            WHERE CONCAT(
                COALESCE(id, ''), 
                COALESCE(variety_name, ''), 
                COALESCE(collector_price, ''), 
                COALESCE(hq_price, ''), 
                COALESCE(collector_comment, ''), 
                COALESCE(hq_comment, ''), 
                COALESCE(outlet_name, '')
            ) LIKE %s
        """

        # Used as the search feature and initialize the values array
        search = "%" + filter['search'] + "%"
        values = [search]


        if filter.get('hq_id'):
            base_query = base_query + " AND hq_id = %s "
            values.append(filter['hq_id'])
        

        if filter.get('region_id'):
            base_query = base_query + " AND area_id = %s "
            values.append(filter['region_id'])


        if filter.get('time_period'):
            base_query = base_query + " AND time_period = %s "
            values.append(filter['time_period'])
        else:
            current_time_period = SettingsModel.get_current_time_period();
            base_query = base_query + " AND time_period = %s "
            values.append(current_time_period)
        
        # get db connection to query
        portal_db_conn = get_portal_db_connection()
        db_cursor = portal_db_conn.cursor()

        db_cursor.execute( base_query, tuple(values) )
        total_records = len(db_cursor.fetchall())

        
        if filter.get('sort_by') in main_columns:

            if filter['sort_desc'] == "true":
                base_query = base_query + f" ORDER BY {filter['sort_by']} DESC "
            else:
                 base_query = base_query + f" ORDER BY {filter['sort_by']} ASC "
        
        if filter.get('page') and filter.get('rows_per_page'):

            if int(filter['rows_per_page']) > 0 :

                offset = int(filter['rows_per_page']) * (int(filter['page']) - 1)
                max_rows = int(filter['rows_per_page'])

                base_query = base_query + f" LIMIT {offset}, {max_rows} "
        
        db_cursor.execute(base_query, tuple(values))
        assignments = db_cursor.fetchall()
        portal_db_conn.close()


        assignments = [
            {
                "id": ass[0],
                "variety_id": ass[1],
                "outlet_id": ass[2],
                "time_period": str(ass[3]),
                "variety_name": ass[4],
                "code": ass[5],
                "outlet_name": ass[6],
                "collector_price": str(ass[7]) if ass[7] else None,
                "collector_comment": ass[8],
                "hq_price": str(ass[9]) if ass[9] else None,
                "hq_comment": ass[10],
                "collected_at" : str(ass[11]) if ass[11] else None,
                "last_collected" : str(datetime.date.today()),
                "request_substitution_status" : None,
                "can_substitute": False,
                "substitution": True if ass[13] else False,
                "substitution_outlet_name": ass[12],
                "substitution_variety_name": ass[13],
                "collector_collected_at": str(ass[15]) if ass[15] else None,
                "status": ass[17],
            } for ass in assignments
        ]


        return {"assignments": assignments, "count": total_records}
    



    @classmethod
    def activateAssuranceAssignments(cls, area_id, hq_id):

        current_time_period = SettingsModel.get_current_time_period()

        query = """
            UPDATE quality_assurance_assignment SET
                status = 'active'
            WHERE assignment_id in (SELECT assignment_id FROM assignment WHERE area_id = %s) 
            AND hq_id = %s
            AND time_period = %s
        """

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (area_id, hq_id, current_time_period))
        conn.commit()
        conn.close()

        return True