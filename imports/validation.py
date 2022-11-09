# check if all columns exist
def required_columns_exist(data, REQUIRED_COLUMNS):

    result = {
        "missing_columns": [],
        "error": False,
    }

    for column in REQUIRED_COLUMNS:
        if column not in data.columns:
            result["missing_columns"].append(column)
    
    if len(result["missing_columns"]) > 0:
        result['error'] = True
        result['message'] = f"Required columns are missing: {result['missing_columns']}"    

    return result