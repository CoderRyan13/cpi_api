from datetime import datetime
from flask_jwt_extended import current_user
from flask_restful import Resource
from db import get_cpi_db_connection, get_portal_db_connection



# This determines if the assignment can be substituted for the assignment time period
def can_substitute_assignment(last_collected, time_period_str):

    time_period = datetime.strptime(time_period_str, "%Y-%m-%d")
    
    
    if last_collected is not None:
        l_year = last_collected.year
        l_month = last_collected.month
        c_year = time_period.year
        c_month = time_period.month
        months_difference = (c_year - l_year) * 12 + c_month - l_month
        return months_difference > 2
    return False



class AssignmentsRawDataLoader(Resource):
    
    def get(self):

        try:

            portal_db = get_portal_db_connection()
            portal_db_cursor = portal_db.cursor()

            cpi_db = get_cpi_db_connection()
            cpi_db_cursor = cpi_db.cursor()


            #get cpi assignments
            query = """ SELECT 
                outlet_product_variety_id, 
                outlet_name, 
                outlet_id,
                variety_id,
                variety_name, 
                previous_price, 
                code, 
                collector_id, 
                collector_name,
                last_collected
            FROM Assignment_View """

            cpi_db_cursor.execute(query)
            cpi_assignments = cpi_db_cursor.fetchall()

            print("CPI ASSIGNMENTS: ", len(cpi_assignments))

            new_assignments = []

            #sync assignments to the collector db
            for assignment in cpi_assignments:
                
                #check if the assignment already exist
                find_query = """SELECT id FROM assignment
                                WHERE outlet_id = %s 
                                AND variety_id = %s 
                                AND collector_id = %s """


                # current_time_period = datetime.today().strftime('%Y-%m-01')

                portal_assignment = portal_db_cursor.execute(find_query, (assignment[2], assignment[3], assignment[7] ))
                portal_assignment= portal_db_cursor.fetchone()
        
                #group items by existence
                if portal_assignment is None:

                    # find the portal outlet and  variety to use that id
                    outlet_query = """SELECT id FROM collector_outlet WHERE cpi_outlet_id = %s """
                    portal_db_cursor.execute(outlet_query, (assignment[2], ))
                    outlet_id = portal_db_cursor.fetchone()
                    outlet_id = outlet_id[0] if outlet_id is not None else None

                    variety_query = """SELECT id FROM collector_variety WHERE cpi_variety_id = %s """
                    portal_db_cursor.execute(variety_query, (assignment[3], ))
                    variety_id = portal_db_cursor.fetchone()
                    variety_id = variety_id[0] if variety_id is not None else None


                    # This Verifies that an Outlet and Variety record Exist For Each Assignment that is being created
                    if outlet_id is None or variety_id is None:
                        return {'message': "Failed To Create Assignment Please Sync Varieties and Outlets then try again!"}, 400


                    new_assignments.append((
                        assignment[0], 
                        outlet_id, 
                        variety_id, 
                        assignment[7], 
                        "active"
                    ))


            create_query = """INSERT INTO assignment(
                        outlet_product_variety_id, 
                        outlet_id,
                        variety_id,
                        collector_id, 
                        status,
                        create_date_time
                    ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"""
              
            portal_db_cursor.executemany(create_query, new_assignments)

            portal_db.commit()  

            new_assignments = [ { 
                "outlet_product_variety_id":assignment[0],
                "outlet_id":assignment[1],
                "variety_id":assignment[2],
                "collector_id":assignment[3],
                "status":assignment[4],
            } for assignment in new_assignments]

            portal_db.close()
            cpi_db.close()

            return { "total": len(new_assignments), "new_assignments": new_assignments }

            
        except Exception as e:
            print(e)
            return "System Error", 500  # internal server error