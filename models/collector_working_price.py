from datetime import datetime
from db import db, get_portal_db_connection
from models.settings import SettingsModel
from models.settings import SettingsModel
from datetime import  datetime


# This includes February, May, August, and November
QUARTERLY_MONTHS = [2, 5, 8, 11]

# Used to verify if the current month is quarterly
def is_quarterly_month():

    #Get the current time period

    current_time_period = SettingsModel.get_current_time_period()
    datetime_now = datetime.strptime(current_time_period, '%Y-%m-%d')

    # verify if the current time period includes only the monthly varieties 
    return datetime_now.month in QUARTERLY_MONTHS


class WorkingPriceModel(db.Model):

    __tablename__ = 'working_price'

    id = db.Column(db.Integer, primary_key=True)
    assignment_id = db.Column(db.Integer, unique=True)
    time_period = db.Column(db.Date, nullable=False)
    time_period_created = db.Column(db.Date, nullable=False)
    date_created = db.Column(db.Date, nullable=False)
    price = db.Column(db.Float, nullable=False)
    generic_outlier = db.Column(db.Float, nullable=True)
    z_score_outlier = db.Column(db.Float, nullable=True)
    inter_quartile_outlier = db.Column(db.Float, nullable=True)
    outlier_status = db.Column(db.Enum('pending', 'approved', 'rejected'), nullable=True)
    flag = db.Column(db.Enum('IMPUTED', 'SUBSTITUTION', 'BACK_HISTORY', 'MANUAL_EDIT'), nullable=True)
   

    def __init__(self, assignment_id, time_period, price, flag=None, _id=None):
        self.id = _id
        self.assignment_id = assignment_id
        self.time_period = time_period
        self.price = price
        self.flag = flag
       

    def json(self):
        return {
            'id': self.id,
            'assignment_id': self.assignment_id,
            "price": str(self.price) if self.price else None,
            "time_period": str(self.time_period),
            'flag': self.flag
        }

    @classmethod
    def find_by_id(cls, id):
        return cls.query.filter_by(id=id).first()


    def update_price(self, price, status):

        if self.price != price:
            self.flag = 'MANUAL_EDIT'

        self.price = price
        self.outlier_status = status
        db.session.commit()


    @classmethod
    def auto_approve_working_prices(cls):

        current_time_period = SettingsModel.get_current_time_period()
        query = f"UPDATE working_price SET outlier_status = 'approved' WHERE (generic_outlier between -5 AND 5 ) AND time_period = '{current_time_period}' "
        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, ())
        conn.commit()
        conn.close()


    @classmethod
    def clean_current_time_period(cls):

        current_time_period = SettingsModel.get_current_time_period()
        cls.query.filter_by(time_period_created=current_time_period).delete()
        # query = """
        #     DELETE FROM working_price WHERE time_period_created = %s
        # """
        # db.session.execute(query, (current_time_period,))
        db.session.commit()

    @classmethod
    def save_assignments_to_db(cls, assignments):

        current_time_period = SettingsModel.get_current_time_period()

        # Used to get the current price of the assignment that are not monthly when the 
        # current_time_period is not a quarter (Big Basket)
        is_not_big_basket = not is_quarterly_month()

        prices_to_generate = []

        for assignment in assignments:

            assignment_id = assignment['id']
            price = assignment['new_price']
            flag = None

            # verify if the assignment is not monthly and it is not a quarter month
            if is_not_big_basket and assignment['is_monthly'] != 1:
                price = assignment['previous_price']
                print("PREVIOUS PRICE :", assignment['previous_price'])


            # Verify if the assignment is a substitution assignment
            if assignment['substitution']:
                assignment_id = assignment['substitution']['id']
                price = assignment['substitution']['price']
                flag = "SUBSTITUTION"

                # If the substitution was rejected then we should just ignore it and use original assignment (IMPUTATION)
                if assignment['substitution']['status'] == 'rejected':
                    flag = "IMPUTED"
                    assignment_id = assignment['id']
                    price = 0.0

            else:

                # If the assignment price is rejected then we need to make IMPUTATION
                if assignment['status'] =='rejected' or (float(assignment['new_price']) == 0):
                    flag = "IMPUTED"
                    price = 0.0

            
            prices_to_generate.append( (assignment_id, current_time_period, price, flag, current_time_period) )
        
        query = "INSERT INTO working_price ( assignment_id, time_period, price, flag, time_period_created ) VALUES ( %s, %s, %s, %s, %s ) "

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.executemany(query, prices_to_generate)
        conn.commit()
        conn.close()
                
    @classmethod
    def verify_all_assignment_approval(cls):

        query = f""" SELECT (
                        SELECT count(*) FROM 
                                        current_time_period_assignments 
                                        WHERE (status in ('approved', 'rejected') OR substitution_status in ('approved', 'rejected'))
                                        { 'AND is_monthly = 1' if is_quarterly_month() == False else '' }
                        ) = 
                        (
                            SELECT count(*) FROM 
                                        current_time_period_assignments
                                        { 'WHERE is_monthly = 1' if is_quarterly_month() == False else '' }
                        )
                """
        
        data_rows = db.session.execute(query, ())
        
        result = data_rows.fetchone()

        print(result[0])

        return result[0] == 1

    @classmethod
    def getCurrentSubstitutionPrices(cls):
        
        current_time_period = SettingsModel.get_current_time_period()

        query = """
                SELECT 
                    assignment_id,	
                    area_id,	
                    variety_id,	
                    product_id,	
                    working_price_id,	
                    price,	
                    time_period,	
                    flag,	
                    assignment_parent_id
                FROM working_price_view 
                WHERE flag = 'SUBSTITUTION' AND time_period = %s
        """

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (current_time_period,))
        substitutions = cursor.fetchall()
        conn.close()

        substitutions = [
            {
                "assignment_id": element[0],
                "area_id": element[1],
                "variety_id": element[2],
                "product_id": element[3],
                "working_price_id": element[4],
                "price": element[5],
                "time_period": str(element[6]),
                "flag": element[7],
                "assignment_parent_id": element[8]
            } for element in substitutions
        ] 

        print(substitutions)

        return substitutions

    @classmethod
    def getPriceByAssignmentId(cls, assignment_id):
        
        current_time_period = SettingsModel.get_current_time_period()

        query = """
                SELECT 
                    assignment_id,	
                    area_id,	
                    variety_id,	
                    product_id,	
                    working_price_id,	
                    price,	
                    time_period,	
                    flag,	
                    assignment_parent_id
                FROM working_price_view 
                WHERE time_period = %s AND assignment_id = %s 
        """

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (current_time_period, assignment_id))
        assignment = cursor.fetchone()
        conn.close()

        print("Assignment")
        print(assignment)


        try:
            return  {
                "assignment_id": assignment[0][0],
                "area_id": assignment[0][1],
                "variety_id": assignment[0][2],
                "product_id": assignment[0][3],
                "working_price_id": assignment[0][4],
                "price": assignment[0][5],
                "time_period": str(assignment[0][6]),
                "flag": assignment[0][7],
                "assignment_parent_id": assignment[0][8]
            } 
        except Exception as e:
            return None


    @classmethod
    def getPriceHistoryByAssignmentId(cls, assignment_id):

        query = """
                SELECT 
                    assignment_id,	
                    area_id,	
                    variety_id,	
                    product_id,	
                    working_price_id,	
                    price,	
                    time_period,	
                    flag,	
                    assignment_parent_id
                FROM working_price_view 
                WHERE assignment_id = %s
                ORDER BY time_period DESC
        """

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (assignment_id,))
        assignment = cursor.fetchall()
        conn.close()

        assignment = [
            {
                "assignment_id": element[0],
                "area_id": element[1],
                "variety_id": element[2],
                "product_id": element[3],
                "working_price_id": element[4],
                "price": element[5],
                "time_period": str(element[6]),
                "flag": element[7],
                "assignment_parent_id": element[8]
            } for element in assignment
        ] 

        return assignment



    @classmethod
    def getUniqueTimePeriods(cls):

        query = """ SELECT DISTINCT time_period FROM working_price ORDER BY time_period DESC"""

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, ())
        time_periods = cursor.fetchall()
        conn.close()

        time_periods = [str(time_period[0]) for time_period in time_periods]

        print(time_periods)

        return time_periods

    @classmethod
    def get_imputation_prices(cls):

        current_time_period = SettingsModel.get_current_time_period()

        query = """ 
            SELECT
                w_price.id,
                w_price.assignment_id,
                w_price.price,
                v.code,
                assignment.area_id,
                assignment.variety_id,
                v.product_id,
                previous_w_price.price
            FROM working_price as w_price
            JOIN assignment ON w_price.assignment_id = assignment.id
            JOIN collector_variety as v on assignment.variety_id = v.id
            JOIN working_price as previous_w_price 
                ON w_price.assignment_id = previous_w_price.assignment_id AND previous_w_price.time_period = (SELECT %s - INTERVAL 1 MONTH)
            WHERE (
                w_price.flag = "IMPUTED"
                OR w_price.outlier_status = 'rejected'
                OR w_price.price = 0
            )
            AND w_price.time_period = %s
        """

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (current_time_period, current_time_period))
        imputation_prices = cursor.fetchall()
        conn.close()

        imputation_prices = [
            {
                "id": price[0],
                "assignment_id": price[1],
                "price": price[2],
                "code": price[3],
                "area_id": price[4],
                "variety_id": price[5],
                "product_id": price[6],
                "previous_price": price[7]
            } for price in imputation_prices
        ]

        print(imputation_prices)

        return imputation_prices

    
    @classmethod
    def getCurrentOutliers(cls, filter):

        current_time_period = SettingsModel.get_current_time_period()
        
        values = [current_time_period, current_time_period]

        base_query = """ 
            SELECT 
                id,
                assignment_id,
                current_price,
                generic_outlier,
                z_score_outlier,
                inter_quartile_outlier,
                outlier_status,
                code,
                area_id,
                variety_id,
                product_id,
                price,
                area_name,
                variety_name,
                outlet_name,
                comment
            FROM (
                
                SELECT
                            w_price.id,
                            w_price.assignment_id,
                            w_price.price as current_price,
                            w_price.generic_outlier,
                            w_price.z_score_outlier,
                            w_price.inter_quartile_outlier,
                            w_price.outlier_status,
                            v.code,
                            assignment.area_id,
                            assignment.variety_id,
                            v.product_id,
                            previous_w_price.price,
                            area.name as area_name,
                            v.name as variety_name,
                            o.est_name as outlet_name,
                            price.comment
                        FROM working_price as w_price
                        JOIN assignment ON w_price.assignment_id = assignment.id
                        JOIN collector_variety as v on assignment.variety_id = v.id
                        JOIN collector_outlet as o on assignment.outlet_id = o.id
                        JOIN collector_area as area on assignment.area_id = area.id
                        JOIN working_price as previous_w_price 
                            ON w_price.assignment_id = previous_w_price.assignment_id AND previous_w_price.time_period = (SELECT %s - INTERVAL 1 MONTH)
                        LEFT JOIN price as price on assignment.id = price.assignment_id AND price.time_period = w_price.time_period
                        WHERE w_price.outlier_status is not null
                        AND w_price.time_period = %s
                        AND CONCAT (assignment.id, w_price.id, v.name, o.est_name, area.name ) LIKE %s
            ) as temp_w_price
            WHERE 1
        """

        
        # Used as the search feature and initialize the values array
        search = "%" + filter['search'] + "%"
        values.append(search)


        if filter.get('outlier_status'):
            base_query = base_query + " AND outlier_status = %s "
            values.append(filter['outlier_status'])
        

        if filter.get('region_id'):
            base_query = base_query + " AND area_id = %s "
            values.append(filter['region_id'])

        print(values)
        # get db connection to query
        portal_db_conn = get_portal_db_connection()
        db_cursor = portal_db_conn.cursor()
        db_cursor.execute( base_query, tuple(values) )
        total_records = len(db_cursor.fetchall())


        if filter['sort_by'] in [
            'id',
            'assignment_id',
            'current_price',
            'generic_outlier',
            'z_score_outlier',
            'inter_quartile_outlier',
            'outlier_status',
            'code',
            'area_id',
            'variety_id',
            'product_id',
            'price',
            'area_name',
            'variety_name',
            'outlet_name',
            'comment'
        ]:

            if filter['sort_desc'] == "true":
                base_query = base_query + f" ORDER BY {filter['sort_by']} DESC "
            else:
                 base_query = base_query + f" ORDER BY {filter['sort_by']} ASC "
        
        if filter['page'] and filter['rows_per_page']:

            if int(filter['rows_per_page']) > 0 :

                offset = int(filter['rows_per_page']) * (int(filter['page']) - 1)
                max_rows = int(filter['rows_per_page'])

                base_query = base_query + f" LIMIT {offset}, {max_rows} "
        
        db_cursor.execute( base_query, tuple(values) )
        records = db_cursor.fetchall()
        portal_db_conn.close()

        outliers = []

        for record in records:
            outliers.append({
               
                "id": record[0],
                "assignment_id": record[1],
                "current_price": record[2],
                "generic_outlier": record[3],
                "z_score_outlier": record[4],
                "inter_quartile_outlier": record[5],
                "outlier_status": record[6],
                "code": record[7],
                "area_id": record[8],
                "variety_id": record[9],
                "product_id": record[10],
                "previous_price": record[11],
                "area_name": record[12],
                "variety_name": record[13],
                "outlet_name": record[14],
                "comment": record[15]
            })

        return {"outliers": outliers, "count": total_records} 




