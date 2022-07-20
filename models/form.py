from db import portal_db

class Form: 

    def get_time_periods():
        query = """
            SELECT DISTINCT time_period FROM assignment ORDER BY time_period DESC
        """
        portal_db.execute(query)
        time_periods = portal_db.fetchall()
        time_periods = [ str(time_period[0]) for time_period in time_periods ]
        return time_periods

    def get_collectors():
        query = """
            SELECT DISTINCT collector_id, collector_name FROM assignment ORDER BY collector_id DESC
        """
        portal_db.execute(query)
        collectors = portal_db.fetchall()
        collectors = [{ 'collector_id': collector[0], 'collector_name': collector[1] } for collector in collectors]
        return collectors
