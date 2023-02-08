
from flask_restful import Resource, request
from models.settings import SettingsModel
from models.collector_working_price import WorkingPriceModel
from validators.errors import ServerError, Validation_Error
from db import get_portal_db_connection, get_sql_alchemy_db_connection
import pandas as pd
import numpy as np
from validators.errors import NotFoundError, ServerError, Validation_Error


class OutlierDetections(Resource):

    def get(self):
        try: 

            # get the query string parameters
            query = request.args.to_dict()

           # get the filters ready
            filter = {
                "search": query.get("search", ''),
                "page": query.get("page", None),
                "rows_per_page": query.get("rows_per_page", None),
                "sort_by": query.get("sort_by", None),
                "sort_desc": query.get("sort_desc", False),
                'region_id': query.get('region_id', None),
                'outlier_status': query.get('outlier_status', None),
            }

            # get the filters validated
            try:

                if filter['page'] and filter['rows_per_page'] :
                    int(filter['page'])
                    int(filter['rows_per_page'])


            except:

                raise Validation_Error("Invalid page or rows_per_page")

            # get the list of outliers and total
            result = WorkingPriceModel.getCurrentOutliers(filter)

            return {"total": result["count"], "outliers": result["outliers"]  }, 200
            
        except Exception as e:
            print(e)
            raise ServerError()


    def post(self):
        process_outlier_detection()
        return True


    def put(self):
        try: 
            
            WorkingPriceModel.auto_approve_working_prices()
            return True, 200

        except Exception as e:
            print(e)
            raise ServerError()


        




def process_outlier_detection():

        current_time_period = SettingsModel.get_current_time_period()

        query = f"""
            SELECT 
                current_price.assignment_id,
                current_price.id as current_price_id,
                previous_price.price as previous_price,
                current_price.price as current_price,
                SUBSTRING(variety.code, 1, 14) as level_4_code,
                ((current_price.price / previous_price.price ) * 100) as stpr,
                assignment.area_id,
                current_price.outlier_status
            FROM working_price as current_price
            JOIN working_price as previous_price ON previous_price.assignment_id = current_price.assignment_id AND previous_price.time_period = (SELECT ('{current_time_period}'  - INTERVAL 1 MONTH))
            JOIN assignment on assignment.id = current_price.assignment_id
            JOIN collector_variety as variety on variety.id = assignment.variety_id
            WHERE current_price.time_period = '{current_time_period}'
            AND current_price.flag is null;
        """
 

        price_data_frame = pd.read_sql(query, get_sql_alchemy_db_connection())

        price_data_frame['generic_outlier'] = None
        price_data_frame['z_score_outlier'] = None
        price_data_frame['z_score_outlier'] = None

        possible_outliers = price_data_frame[ (price_data_frame['stpr'] != 100.0 ) &  ( price_data_frame['outlier_status'] != 'rejected' ) &  ( price_data_frame['outlier_status'] != 'approved' ) ]
        print("🚀 ~ file: outlier_detection.py:47 ~ possible_outliers", possible_outliers)

        outliers = []

        for price in possible_outliers.itertuples():

            generic_outlier = None
            z_score_outlier = None
            inter_quartile_outlier = None


            # OUTLIER USING GENERIC DETECTION
            generic_outlier = price.stpr - 100

            # OUTLIER USING Z-SCORE DETECTION

            # Find the prices by  level 4 code and area to find the stpr percentage average and Standard deviation 
            level_4_products_by_area = price_data_frame[  (price_data_frame['level_4_code'] == price.level_4_code  ) &  (price_data_frame['area_id'] == price.area_id  ) ]


            level_4_average = level_4_products_by_area["stpr"].mean()
            level_4_standard_deviation = level_4_products_by_area["stpr"].std()

            # print("AVERAGE: ", level_4_average, "STD: ", level_4_standard_deviation)

            z_score_outlier = (price.stpr - level_4_average) / level_4_standard_deviation
            z_score_outlier = None if pd.isna(z_score_outlier) else z_score_outlier

            print("ASSIGNMENT_ID: ", price.assignment_id, "     STPR: ", price.stpr, "   GENERIC OUTLIER: ", price.stpr - 100, "  LEVEL_4_CODE: ", price.level_4_code, '  MEAN: ', level_4_average, '  STD: ', level_4_standard_deviation, '  z_score: ', z_score_outlier)



            # OUTLIER USING IQR DETECTION

            quartile_1 = np.percentile(level_4_products_by_area['stpr'] , 25)
            quartile_3 = np.percentile(level_4_products_by_area['stpr'] , 75)

            IQR = quartile_3 - quartile_1
            ul = quartile_3 + 1.5 * IQR
            ll = quartile_1- 1.5 * IQR


            if price.stpr > ul:
                inter_quartile_outlier =price.stpr - ul

            if price.stpr < ll:
                inter_quartile_outlier = ll - price.stpr

            # Verify if there is a difference in quartile limits
            outliers.append((generic_outlier, z_score_outlier, inter_quartile_outlier, 'pending', price.current_price_id))

        
        query = """
            UPDATE working_price SET generic_outlier = %s, z_score_outlier = %s, inter_quartile_outlier =%s, outlier_status =%s WHERE id = %s 
        """

        print(outliers)

        conn = get_portal_db_connection()
        cursor = conn.cursor()
        cursor.executemany(query, outliers)
        conn.commit()
        conn.close()

        # possible_outliers.to_csv('C:\\Users\\spalma\\Desktop\\clean_data.csv', encoding='utf-8', index=False)




class OutlierDetection(Resource):

    def put(self, id):

        try: 
        
            raw_data = request.get_json()

            print(raw_data)

            if not raw_data.get('price', None) or not raw_data.get('outlier_status', None):
                raise Validation_Error()

            working_price = WorkingPriceModel.find_by_id(id)

            print(working_price)

            if not working_price:
                raise NotFoundError()

            working_price.update_price(raw_data.get('price') , raw_data.get('outlier_status'))

            return True, 200

        except Exception as e:
            print(e)
            raise ServerError()
