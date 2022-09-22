from db import get_portal_db_connection

class Form: 

    def get_time_periods():

        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        query = """
            SELECT DISTINCT time_period FROM assignment ORDER BY time_period DESC
        """
        portal_db_cursor.execute(query)
        time_periods = portal_db_cursor.fetchall()
        time_periods = [ str(time_period[0]) for time_period in time_periods ]
        portal_db.close()
        return time_periods

    def get_collectors():

        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        query = """
            SELECT 
                id,
                name,
                email,
                username,
                type,
                area_id            
            FROM user
            ORDER BY id DESC
        """
        portal_db_cursor.execute(query)
        collectors = portal_db_cursor.fetchall()
        print(collectors)
        collectors = [{ 
            "id": collector[0],
            "name": collector[1],
            "email": collector[2],
            "username": collector[3],
            "type": collector[4],
            "area_id"  : collector[5]
        } for collector in collectors]
        portal_db.close()
        return collectors
 