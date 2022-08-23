from datetime import datetime
from flask_jwt_extended import current_user
from flask_restful import Resource
from db import cpi_db, portal_db_connection, portal_db



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
            cpi_db.execute(query)
            cpi_assignments = cpi_db.fetchall()

            print("CPI ASSIGNMENTS: ", len(cpi_assignments))

            new_assignments = []

            #sync assignments to the collector db
            for assignment in cpi_assignments:
                
                #check if the assignment already exist
                find_query = """SELECT id FROM assignment
                                WHERE outlet_product_variety_id = %s 
                                AND time_period = %s 
                                AND collector_id = %s """
                
                current_time_period = datetime.today().strftime('%Y-%m-01')

                portal_assignment = portal_db.execute(find_query, (assignment[0], current_time_period, assignment[7] ))
                portal_assignment= portal_db.fetchone()
        
                #group items by existence
                if portal_assignment is None:

                    print(type(assignment[9]), type(current_time_period))

                    # find the portal outlet and  variety to use that id
                    outlet_query = """SELECT id FROM collector_outlet WHERE cpi_outlet_id = %s """
                    portal_db.execute(outlet_query, (assignment[2], ))
                    outlet_id = portal_db.fetchone()
                    outlet_id = outlet_id[0] if outlet_id is not None else None

                    variety_query = """SELECT id FROM collector_variety WHERE cpi_variety_id = %s """
                    portal_db.execute(variety_query, (assignment[3], ))
                    variety_id = portal_db.fetchone()
                    variety_id = variety_id[0] if variety_id is not None else None


                    # This Verifies that an Outlet and Variety record Exist For Each Assignment that is being created
                    if outlet_id is None or variety_id is None:
                        return {'message': "Failed To Create Assignment Please Sync Varieties and Outlets then try again!"}, 400


                    new_assignments.append((
                        assignment[0], 
                        assignment[1], 
                        outlet_id, 
                        variety_id, 
                        assignment[4], 
                        assignment[5], 
                        assignment[6], 
                        assignment[7], 
                        assignment[8], 
                        assignment[9],
                        can_substitute_assignment(assignment[9], current_time_period),
                        current_time_period,
                        None,
                        None,
                        datetime.now(),
                        None,
                        'inactive'
                    ))


            create_query = """INSERT INTO assignment(
                        outlet_product_variety_id, 
                        outlet_name, 
                        outlet_id,
                        variety_id,
                        variety_name, 
                        previous_price, 
                        code, 
                        collector_id, 
                        collector_name,
                        last_collected,
                        can_substitute,
                        time_period, 
                        approved_by, 
                        date_approved,  
                        create_date_time, 
                        update_date_time,
                        status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
              
            portal_db.executemany(create_query, new_assignments)

            portal_db_connection.commit()  

            new_assignments = [ { 
                "outlet_product_variety_id":assignment[0],
                "outlet_name":assignment[1],
                "outlet_id":assignment[2],
                "variety_id":assignment[3],
                "variety_name":assignment[4],
                "previous_price": assignment[5],
                "code":assignment[6],
                "collector_id":assignment[7],
                "collector_name":assignment[8],
                "last_collected": str(assignment[9]),
                "can_substitute": assignment[10],
                "time_period": str(assignment[11]),
                "approved_by": assignment[12],
                "date_approved": assignment[13],
                "create_date_time": str(assignment[14]),
                "update_date_time": assignment[15],
                "status":assignment[16]
            } for assignment in new_assignments]

            return { "total": len(new_assignments), "new_assignments": new_assignments }

            
        except Exception as e:
            print(e)
            return "System Error", 500  # internal server error