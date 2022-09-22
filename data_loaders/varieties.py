from datetime import datetime
from flask_restful import Resource
from db import get_portal_db_connection,  get_cpi_db_connection 

class VarietiesRawDataLoader(Resource):
    
    def get(self):

        try:
        
            cpi_db = get_cpi_db_connection()
            cpi_db_cursor = cpi_db.cursor()

            portal_db = get_portal_db_connection()
            portal_db_cursor = portal_db.cursor()

            #get cpi varieties
            cpi_db_cursor.execute("SELECT id, code, name, product_id FROM variety")
            cpi_varieties = cpi_db_cursor.fetchall()

            #results
            synced_varieties = {
                "updated_varieties" : [],
                "new_varieties" : [],
            }

            new_varieties = []
            updated_varieties = []


            #sync products to the collector db
            for variety in cpi_varieties:
                
                #check if the variety already exist
                find_query = "SELECT cpi_variety_id FROM collector_variety WHERE cpi_variety_id = %s"
                portal_variety = portal_db_cursor.execute(find_query, (variety[0],))
                portal_variety= portal_db_cursor.fetchone()

                #group items by existence
                if portal_variety is None:
                    new_varieties.append((variety[0], variety[1], variety[2], variety[3], 1001, datetime.now()))

                else:
                    updated_varieties.append(( variety[1], variety[2], variety[3], variety[0]))

            print(new_varieties)
            print(updated_varieties)
            
            #create the new varieties
            create_query = """ INSERT INTO collector_variety(cpi_variety_id, code, name, product_id, approved_by, date_approved)
                        VALUES(%s, %s, %s, %s, %s, %s)
                    """
            portal_db_cursor.executemany(create_query, new_varieties)
            portal_db.commit()

            #update the existing varieties
            update_query = """ UPDATE collector_variety SET 
                                code = %s,  
                                name = %s, 
                                product_id = %s, 
                                WHERE cpi_variety_id = %s"""

            portal_db_cursor.executemany(update_query, updated_varieties)
            portal_db.commit()
            
            synced_varieties['new_varieties'] = [{ 
                'id': variety[0], 
                'code': variety[1], 
                'name': variety[2], 
                'product_id': variety[3], 
            } for variety in new_varieties]

            synced_varieties['updated_varieties'] = [{ 
                'id': variety[3], 
                'code': variety[0], 
                'name': variety[1], 
                'product_id': variety[2],
            } for variety in updated_varieties]

            cpi_db.close()
            portal_db.close()

            return synced_varieties
        
        except Exception as e:
            print(e)
            return "System Error", 500  # internal server error