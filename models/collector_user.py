from db import db

class CollectorUserModel(db.Model):
    
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(255), nullable=False)
    username = db.Column(db.String(255), nullable=False)
    password = db.Column(db.Text, nullable=False)
    area_id = db.Column(db.Integer, db.ForeignKey('collector_area.id'), nullable=True)
    type = db.Column(db.Enum('collector', 'HQ'), nullable=False)
    status = db.Column(db.Enum('activated', 'deactivated'), nullable=False)

    def __init__(self, name, email, username, password, area_id, type, status):
        self.name = name
        self.email = email
        self.username = username
        self.password = password
        self.area_id = area_id
        self.type = type
        self.status = status

    def __repr__(self):
        return '<CollectorUserModel %r>' % self.name

    def json(self):
        return {
            'id': self.id,
            'name': self.name,
            'email': self.email,
            'username': self.username,
            'password': None,
            'area_id': self.area_id,
            'type': self.type,
            'status': self.status
        }

    @classmethod
    def find_by_username(cls, username):
        return cls.query.filter_by(username=username).first()
    
    @classmethod
    def find_by_email(cls, email):
        return cls.query.filter_by(email=email).first()

    @classmethod
    def find_by_id(cls, _id):
        return cls.query.filter_by(id=_id).first()
    
    def save_to_db(self):
        db.session.add(self)
        db.session.commit()
    
    def update_password(self, new_password):
        self.password = new_password
        db.session.commit()

    def update(self, user_data):
        self.name = user_data['name']
        self.username = user_data['username']
        self.email = user_data['email']
        self.type = user_data['type']
        self.area_id = user_data['area_id'] if user_data['type'] == 'collector' else None
        self.status = user_data['status']
        db.session.commit()

    @classmethod
    def find_all(cls):
        return cls.query.all()
    