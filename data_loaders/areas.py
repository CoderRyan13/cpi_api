from flask_restful import Resource
from db import cpi_db, portal_db_connection, portal_db

class AreasRawDataLoader(Resource):
    
    def get(self):

        try:
        
            #get cpi areas
            cpi_db.execute("SELECT id, area_name, areaid FROM area")
            cpi_areas = cpi_db.fetchall()

            #results
            synced_areas = {
                "updated_areas" : [],
                "new_areas" : [],
            }

            new_areas = []
            updated_areas = []


            #sync areas to the collector db
            for area in cpi_areas:
                
                #check if the area already exist
                find_query = "SELECT id FROM collector_area WHERE id = %s"
                portal_area = portal_db.execute(find_query, (area[0],))
                portal_area= portal_db.fetchone()

                #group items by existence
                if portal_area is None:
                    new_areas.append((area[0], area[1], area[2]))

                else:
                    updated_areas.append((area[1], area[2], area[0]))

            #create the new areas
            create_query = """ INSERT INTO collector_area(id, name, areaid) VALUES(%s, %s, %s)"""
            portal_db.executemany(create_query, new_areas)
            portal_db_connection.commit()

            #update the existing areas
            update_query = """ UPDATE collector_area SET name = %s,  areaid = %s WHERE id = %s"""
            portal_db.executemany(update_query, updated_areas)
            portal_db_connection.commit()
            
            synced_areas['new_areas'] = [{ 
                'id': area[0], 
                'name': area[1], 
                'areaid': area[2] 
            } for area in new_areas]

            synced_areas['updated_areas'] = [{ 
                'id': area[2], 
                'name': area[0], 
                'areaid': area[1] 
            } for area in updated_areas]

            return synced_areas
        
        except Exception as e:
            return "System Error", 500  # internal server error