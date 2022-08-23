
from datetime import datetime
import os
from flask import request
from flask_restful import Resource
from werkzeug.utils import secure_filename

from validators.errors import ServerError

ALLOWED_EXTENSIONS = ['.csv']
UPLOAD_PATH = "imports"

class UploadExcel(Resource):
   
    def post(self):

        try: 
            # check if the post request has the file part and the name is secure
            file = request.files['file']
            filename = secure_filename(file.filename)

            if filename != '':
                
                #Get the file extension and verify if it is allowed
                file_ext = os.path.splitext(filename)[1]
                if file_ext not in ALLOWED_EXTENSIONS:
                    return {"error": True, "message": "File type not accepted!" } , 400

                #Save the file to the imports folder
                filename = f"{datetime.today().strftime('%Y-%m-%d-%H-%M-%S')}-{filename}"
                file.save(os.path.join(UPLOAD_PATH, filename))
                return {"success": True, "message": "File uploaded successfully!", "filename": filename } , 201
            
            return {"error": True, "message": "Please Select a file!"}, 400
        except Exception as e:
            raise ServerError()

       