
from flask_restful import Resource
from models.collector_assignment import AssignmentModel
from models.collector_working_price import WorkingPriceModel
from flask_jwt_extended import jwt_required
from db import get_portal_db_connection
from models.settings import SettingsModel

class SubstitutionBackHistory(Resource):
    
   
    # @jwt_required() 
    def get(self):
        
        # get all the assignments
        substitutions = WorkingPriceModel.getCurrentSubstitutionPrices()

        # get current time period
        current_time_period = SettingsModel.get_current_time_period()

        print("___________________________________________________________________________________________________________________________________________")
        print("SUBS")
        for substitution in substitutions:
            handleSubstitutionBackHistory(substitution, current_time_period)
        
        
        return { 'total': len(substitutions), "success": True}



def handleSubstitutionBackHistory(substitution, current_time_period):
    
    # Get the product stpr either by municipality or national for substitution link
    stpr = getProductSTPR(substitution["product_id"], substitution["area_id"])
    print("Product STPR: " + str(stpr))

    # get the price history for the item being substituted
    substituted_item_price_history = WorkingPriceModel.getPriceHistoryByAssignmentId(substitution['assignment_parent_id'])

    print("Old Assignment History: " )
    print(substituted_item_price_history)

    # Since this is a Substitution there is no price history 
    # for the item being substituted for this period. Therefore,
    # the price for this period is calculated using STPR (previous price * STPR) 
    
    substituted_item_current_price = stpr * substituted_item_price_history[0]['price']
    substituted_item_price_history.insert(0, { "price" : substituted_item_current_price, "time_period": current_time_period })

    print("NEW IIIIIIIIIIIIIIIIIIIIIIIMPPPPPPPPPPPPUTTTTTED PRICE: ", substituted_item_current_price)

    print("OLD Assignment History with IMPUTED current Price: " )
    print(substituted_item_price_history)

    # Now that we have a collection of previous prices for the item being substituted
    # the history can now be calculated 

    
    substitution_history = [
        {
            "time_period": current_time_period,
            "price": substitution['price']
        }
    ]


    # Loop through the history of the item being substituted

    for x in range(len(substituted_item_price_history)-1):

        old_item_current_price = substituted_item_price_history[x]["price"]
        old_item_previous_price = substituted_item_price_history[x+1]["price"]
        time_period = substituted_item_price_history[x+1]["time_period"]

        new_item_current_price = substitution_history[x]["price"]
        
        generated_price = ( old_item_previous_price / old_item_current_price ) * new_item_current_price

        substitution_history.append({
            "time_period": time_period,
            "price": generated_price
        })

        
    # Note that when the substitution history was created we initialized 
    # it with the existing price for this period hence we need to exclude

    substitution_history.pop(0)


    # Create the substitution history in working price table
    createSubstitutionHistory(substitution_history, substitution['assignment_id'], current_time_period)


    print("THE HISTORY THAT NEEDS TO BE CREATED IS AS FOLLOW")
    print(substitution_history)
        

def createSubstitutionHistory( history, assignment_id, current_time_period ):
    
    working_prices = []

    for element in reversed(history):

        working_prices.append((assignment_id, element['time_period'], round(element['price'], 2), "BACK_HISTORY", current_time_period))
    
    conn = get_portal_db_connection()
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO working_price (assignment_id, time_period, price, flag, time_period_created) VALUES ( %s, %s, %s, %s, %s)", working_prices)
    conn.commit()
    conn.close()


def getProductSTPR(product_id, area_id):
    
    # Find all prices for a specific product for this time period and area_id (Municipality Level)
    # Exclude prices that are to imputed 

    query = getStprAverageQuery("AND current_prices.product_id = %s AND current_prices.area_id = %s")
    
    conn = get_portal_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, (product_id, area_id))
    price_averages = cursor.fetchone() 

    current_price_average = price_averages[0]
    previous_price_average = price_averages[1]

    print(current_price_average)
    print(previous_price_average)


    # Verify if the results where Empty. If Empty, try again without area_id check (National Level)
    # Exclude prices that are to imputed 

    if current_price_average == None or previous_price_average == None:

        query = getStprAverageQuery("AND current_prices.product_id = %s")
    
        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (product_id,))
        price_averages = cursor.fetchone() 

        current_price_average = price_averages[0]
        previous_price_average = price_averages[1]


    # Find the average of the previous prices and the current prices. Divide current AVG / previous AVG 
    print(product_id, area_id)
    print("CURRENT PRICES AVERAGE: " + str(current_price_average))
    print("PREVIOUS PRICES AVERAGE: " + str(previous_price_average))

    final_stpr = current_price_average / previous_price_average

    print("STPR: " + str(final_stpr))


    # return STPR TODO: THIS SHOULD REGISTER AN ERROR WHEN THE STPR IS NOT VALID OR FAILS

    return final_stpr


def getStprAverageQuery(check=""):


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
            LEFT JOIN  working_price_view as previous_prices on (current_prices.assignment_id = previous_prices.assignment_id AND previous_prices.time_period = (SELECT ( SELECT value FROM settings WHERE id = 1001 LIMIT 1 ) - INTERVAL 1 MONTH))
            WHERE current_prices.time_period = ( SELECT value FROM settings WHERE id = 1001 LIMIT 1 )
            AND current_prices.flag is null 
            {check}
        ) as curr_prev_prices
    """



    