
from datetime import datetime
from db import db

class RequestedSubstitutionModel(db.Model):

    __tablename__ = 'requested_substitution'
    id = db.Column(db.Integer, primary_key=True)
    assignment_id = db.Column(db.Integer, db.ForeignKey('assignment.id'), nullable=False, unique=True)
    status = db.Column(db.Enum('pending', 'approved', 'rejected'), nullable=False)
    requested_at = db.Column(db.DateTime, db.ForeignKey('collector_outlet.id'), nullable=False)
    updated_at = db.Column(db.DateTime, nullable=True)

    
    def __init__(self, assignment_id):
        self.assignment_id = assignment_id
        self.requested_at = datetime.now()
        self.status = "pending"
        
    def json(self):
        return {
            'id': self.id, 
            'assignment_id': self.assignment_id, 
            'requested_at': str(self.requested_at) if self.requested_at else None, 
            'status': self.status,
            'updated_at': str(self.updated_at) if self.updated_at else None
        }

    @classmethod
    def find_all(cls):
        return cls.query.all()

    
    @classmethod
    def find_by_id(cls, id):
        return cls.query.filter_by(id=id).first()
    
    @classmethod
    def find_by_assignment_id(cls, assignment_id):
        return cls.query.filter_by(assignment_id=assignment_id).first()
    
    def save_to_db(self):

        req_subs = self.find_by_assignment_id(self.assignment_id)

        if req_subs is None:
            db.session.add(self)
            db.session.commit()


    def update_request_by_id(self, status):
        self.status = status
        db.session.commit()


    


    
    
        


