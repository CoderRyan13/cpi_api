
from werkzeug.exceptions import HTTPException


class NotFoundError(HTTPException):
    def __init__(self, resource):
        self.message = f"{resource} not found"
        self.code = 404
        self.description = "The requested resource was not found!"


class ServerError(HTTPException):
    def __init__(self):
        self.message = f"Internal Server Error"
        self.code = 500
        self.description = "Something went wrong!"

class Validation_Error(HTTPException):
    def __init__(self):
        self.message = f"Bad Request, Validation Error"
        self.code = 400
        self.description = "Request payload is invalid!"