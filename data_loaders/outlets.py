from flask_restful import Resource
from db import cpi_db, portal_db_connection, portal_db

class OutletsRawDataLoader(Resource):
    
    def get(self):

        try:
        
            #get cpi outlets
        
            query = """ SELECT 
                            id, 
                            est_name, 
                            note, 
                            address, 
                            phone, 
                            area_id 
                        FROM outlet """
        
            cpi_db.execute(query)
            cpi_outlets = cpi_db.fetchall()

            #results
            synced_outlets = {
                "updated_outlets" : [],
                "new_outlets" : [],
            }

            new_outlets = []
            updated_outlets = []

            #sync outlets to the collector db
            for outlet in cpi_outlets:
                
                #check if the outlet already exist
                find_query = "SELECT id FROM collector_outlet WHERE cpi_outlet_id = %s"
                portal_outlet = portal_db.execute(find_query, (outlet[0],))
                portal_outlet= portal_db.fetchone()

                #group items by existence
                if portal_outlet is None:
                    new_outlets.append((outlet[0], outlet[1], outlet[2], outlet[3], outlet[4], outlet[5]))

                else:
                    updated_outlets.append((outlet[1], outlet[2], outlet[3], outlet[4], outlet[5], outlet[0]))

            print(new_outlets)
            print(updated_outlets)
            
            #create the new outlets
            create_query = """INSERT INTO collector_outlet(
                                cpi_outlet_id, 
                                est_name, 
                                note, 
                                address, 
                                phone, 
                                area_id) 
                            VALUES(%s, %s, %s, %s, %s, %s)"""

            portal_db.executemany(create_query, new_outlets)
            portal_db_connection.commit()

            #update the existing outlets
            update_query = """  UPDATE collector_outlet SET 
                                    est_name=%s, 
                                    note=%s, 
                                    address=%s, 
                                    phone=%s, 
                                    area_id=%s 
                                WHERE cpi_outlet_id=%s"""

            portal_db.executemany(update_query, updated_outlets)
            portal_db_connection.commit()
            
            synced_outlets['new_outlets'] = [{ 
                "cpi_outlet_id" : outlet[0],
                "est_name" : outlet[1],
                "note" : outlet[2],
                "address" : outlet[3],
                "phone" : outlet[4],
                "area_id" : outlet[5]
            } for outlet in new_outlets]

            synced_outlets['updated_outlets'] = [{ 
                "cpi_outlet_id" : outlet[5],
                "est_name" : outlet[0],
                "note" : outlet[1],
                "address" : outlet[2],
                "phone" : outlet[3],
                "area_id" : outlet[4]
            } for outlet in updated_outlets]

            return synced_outlets
        
        except Exception as e:
            print(e)
            return "System Error", 500  # internal server error