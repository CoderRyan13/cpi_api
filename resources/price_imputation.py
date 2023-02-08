
from flask_restful import Resource
from models.collector_working_price import WorkingPriceModel
from models.settings import SettingsModel
from db import get_portal_db_connection
 

# This returns a query

def get_stpr_average_query(check=""):

    return f"""
         SELECT 
            AVG(current_price) as current_price_average,
            AVG(previous_price) as previous_price_average 
        FROM (
            SELECT 
                current_prices.product_id,
                current_prices.assignment_id,
                current_prices.working_price_id as current_working_price_id,
                current_prices.time_period as current_time_period,
                current_prices.price as current_price,
                previous_prices.working_price_id as previous_working_price_id,
                previous_prices.time_period as previous_time_period,
                previous_prices.price as previous_price
            FROM 
            working_price_view as current_prices
            LEFT JOIN  working_price_view as previous_prices on (current_prices.assignment_id = previous_prices.assignment_id AND previous_prices.time_period = (SELECT ( %s ) - INTERVAL 1 MONTH))
            WHERE current_prices.time_period = ( %s )
            AND current_prices.flag is null 
            {check}
        ) as curr_prev_prices
    """



class PriceImputation(Resource):

    
    def get(self):

        # get current time_period from db
        current_time_period = SettingsModel.get_current_time_period()

        # get the missing prices for the current time period on the working price table
        current_missing_prices = WorkingPriceModel.get_imputation_prices()

        # results from the imputation
        values = []

        # loop through the assignments
        for missing_price in current_missing_prices:
            imputation_record = impute_price(missing_price, current_time_period)
            values.append((imputation_record['imputed_price'], imputation_record['working_price_id']))

        query = "UPDATE working_price SET price = %s WHERE id = %s"

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.executemany(query, values)
        conn.commit()
        conn.close()

        return {"updated": len(values)}, 200



def impute_price(price, current_time_period):

    #Initialize the stpr to None
    stpr = None

    # Get the STPR by variety (LEVEL 8) on the municipal level
    stpr = get_level_8_stpr_municipal_level(price, current_time_period)
    
    if not stpr:
        # Get the STPR by variety (LEVEL 8) on the national level
        stpr = get_level_8_stpr_national_level(price, current_time_period)

    if not stpr:
        # Get the STPR by product (LEVEL 6) on the Municipal level
        stpr = get_level_6_stpr_municipal_level(price, current_time_period)



    return {
        "working_price_id": price['id'],
        "assignment_id": price['assignment_id'],
        "code": price['code'],
        "area_id": price['area_id'],
        "variety_id": price['variety_id'],
        "product_id": price['product_id'],
        "previous_price" : price['previous_price'],
        "stpr": stpr,
        "imputed_price": stpr * price['previous_price'] if stpr else None,
    }


def get_level_8_stpr_municipal_level(price, current_time_period):

    # get the previous and current price average for the price history of variety Id
    # and area id as that of the current missing price
    check =  "AND current_prices.variety_id = %s AND current_prices.area_id = %s"
    values = (current_time_period, current_time_period, price['variety_id'], price['area_id'])
    query = get_stpr_average_query(check)

    conn = get_portal_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, values)
    price_averages = cursor.fetchone() 
    conn.close()

    current_price_average = price_averages[0]
    previous_price_average = price_averages[1]

    # if the both the previous or the current price average exist then return STPR
    # else return None to flag that the stpr does not exist
    if current_price_average and previous_price_average:
        return current_price_average / previous_price_average

    return None

  
def get_level_8_stpr_national_level(price, current_time_period):
    
    # get the previous and current price average for the price history of variety Id
    check =  "AND current_prices.variety_id = %s"
    values = (current_time_period, current_time_period, price['variety_id'])
    query = get_stpr_average_query(check)

    conn = get_portal_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, values)
    price_averages = cursor.fetchone() 
    conn.close()

    current_price_average = price_averages[0]
    previous_price_average = price_averages[1]

    # if the both the previous or the current price average exist then return STPR
    # else return None to flag that the stpr does not exist
    if current_price_average and previous_price_average:
        return current_price_average / previous_price_average

    return None


def get_level_7_stpr_municipal_level(price, current_time_period):
    
    # get the previous and current price average for the price history of variety Id
    check =  "AND current_prices.product_id = %s AND current_prices.area_id = %s"
    values = (current_time_period, current_time_period, price['product_id'], price['area_id'])
    query = get_stpr_average_query(check)

    conn = get_portal_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, values)
    price_averages = cursor.fetchone() 
    conn.close()

    current_price_average = price_averages[0]
    previous_price_average = price_averages[1]

    # if the both the previous or the current price average exist then return STPR
    # else return None to flag that the stpr does not exist
    if current_price_average and previous_price_average:
        return current_price_average / previous_price_average

    return None


def get_level_7_stpr_national_level(price, current_time_period):

    # get the previous and current price average for the price history of variety Id
    check =  "AND current_prices.product_id = %s"
    values = (current_time_period, current_time_period, price['product_id'])
    query = get_stpr_average_query(check)

    conn = get_portal_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, values)
    price_averages = cursor.fetchone() 
    conn.close()

    current_price_average = price_averages[0]
    previous_price_average = price_averages[1]

    # if the both the previous or the current price average exist then return STPR
    # else return None to flag that the stpr does not exist
    if current_price_average and previous_price_average:
        return current_price_average / previous_price_average

    return None


# NOTE THIS GETS THE STPR FOR LEVEL 6 (LEVEL 7 BY MUNICIPALITY AND NATIONAL LEVEL INCLUSIVE) 
def get_level_6_stpr_municipal_level(price, current_time_period):
    
    # First We need to find the all the nearest neighbor of the product using the level 6 codes including level 7 of the variety
    # Start by getting the products that start with the level code 6 ordered in asc by absolute difference
    
    # get the level six code from the variety code
    level_6_code = price['code'][0:20]

    # get the level 7 digits from the variety code
    level_7_digits = price['code'][21:23]

    level_6_digits = price['code'][18:20]



    print("HARD-ONE", level_6_code, level_6_digits, level_7_digits)

    # get the level 7 codes to process grouped by level six note that the variety product level is inclusive
    query = f"""
        SELECT 
            id,
            code, 
            description,
            ABS( CONVERT('{level_7_digits}' , SIGNED INTEGER ) - CONVERT(SUBSTRING(code, 22, 2 ) , SIGNED INTEGER ) ) as level_7_abs_diff,
            ABS( CONVERT('{level_6_digits}' , SIGNED INTEGER ) - CONVERT(SUBSTRING(code, 19, 2 ) , SIGNED INTEGER ) ) as level_6_abs_diff
        FROM collector_product 
        WHERE code LIKE '{level_6_code}%'
        ORDER BY level_6_abs_diff ASC, level_7_abs_diff;
    """

    conn = get_portal_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, ())
    products = cursor.fetchall()
    
    stpr = None

    # find the stpr of the products by municipal and national level return stpr if any
    for product in products:

        # tries to find the stpr of the product on the municipal level
        stpr = get_level_7_stpr_municipal_level({
            'product_id': product[0],
            'area_id': price['area_id']
        }, current_time_period)


        # if the product produces an stpr then break the loop
        if stpr : break


        # tries to find the stpr of the product on the national level
        stpr = get_level_7_stpr_national_level({
            'product_id': product[0],
        }, current_time_period)

        # if the product produces an stpr then break the loop
        if stpr: break


    return stpr




# SELECT 
#             id,
#             code, 
#             description,
#             ABS( CONVERT('01' , SIGNED INTEGER ) - CONVERT(SUBSTRING(code, 22, 2 ) , SIGNED INTEGER ) ) as level_7_abs_diff,
#             ABS( CONVERT('01' , SIGNED INTEGER ) - CONVERT(SUBSTRING(code, 19, 2 ) , SIGNED INTEGER ) ) as level_6_abs_diff
#         FROM collector_product 
#         WHERE code LIKE '01.01.01.01.01.01%'
#         ORDER BY level_6_abs_diff ASC, level_7_abs_diff;