from flask_restful import Resource
from db import cpi_db, portal_db_connection, portal_db

class ProductsRawDataLoader(Resource):
    
    def get(self):

        try:
        
            #get cpi products
            cpi_db.execute("SELECT id, code, status, description, create_date_time, update_date_time FROM coicop")
            cpi_products = cpi_db.fetchall()

            #results
            synced_products = {
                "updated_products" : [],
                "new_products" : [],
            }

            new_products = []
            updated_products = []


            #sync products to the collector db
            for product in cpi_products:
                
                #check if the product already exist
                find_query = "SELECT id FROM collector_product WHERE id = %s"
                portal_product = portal_db.execute(find_query, (product[0],))
                portal_product= portal_db.fetchone()

                #group items by existence
                if portal_product is None:
                    new_products.append((product[0], product[1], product[2], product[3], product[4], product[5]))

                else:
                    updated_products.append(( product[1], product[2], product[3], product[4], product[5], product[0]))

            print(new_products)
            print(updated_products)
            
            #create the new products
            create_query = """ INSERT INTO collector_product(id, code,  status, description, created_at, updated_at)
                        VALUES(%s, %s, %s, %s, %s, %s)
                    """
            portal_db.executemany(create_query, new_products)
            portal_db_connection.commit()

            #update the existing products
            update_query = """ UPDATE collector_product SET 
                                code = %s,  
                                status = %s, 
                                description = %s, 
                                created_at = %s, 
                                updated_at = %s
                                WHERE id = %s"""

            portal_db.executemany(update_query, updated_products)
            portal_db_connection.commit()
            
            synced_products['new_products'] = [{ 
                'id': product[0], 
                'code': product[1], 
                'status': product[2], 
                'description': product[3], 
                'created_at': str(product[4]),
                'updated_at': str(product[5]) if product[5] else None 
            } for product in new_products]

            synced_products['updated_products'] = [{ 
                'id': product[5], 
                'code': product[0], 
                'status': product[1], 
                'description': product[2], 
                'created_at': str(product[3]),
                'updated_at': str(product[4]) if product[4] else None
            } for product in updated_products]

            return synced_products
        
        except Exception as e:
            return "System Error", 500  # internal server error