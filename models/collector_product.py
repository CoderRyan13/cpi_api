from datetime import date, datetime
import itertools
from string import ascii_lowercase
from db import db
import models.collector_variety  as CV

class CollectorProductModel(db.Model):

    __tablename__ = 'collector_product'

    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(80), nullable=False)
    description = db.Column(db.String(80), nullable=False)
    status = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime,  nullable=False, default=datetime.now)
    updated_at = db.Column(db.DateTime,  nullable=True)
    varieties = db.relationship("CollectorVarietyModel", backref="product")
    
    def __init__(self, code, description, status, created_at=None, updated_at=None, varieties=None, _id=None):
        self.id = _id
        self.code = code
        self.description = description
        self.status = status
        self.created_at = created_at
        self.updated_at = updated_at
        self.varieties = [] if varieties is None else varieties
        
    def __str__(self):
        return str(self.json())


    def json(self):
        return {
            'id': self.id,
            'code': self.code,
            'description': self.description,
            'status': self.status,
            'created_at': str(self.created_at),
            'updated_at': str(self.updated_at) if self.updated_at else None,
            "varieties": [variety.json() for variety in self.varieties] if self.varieties else [],
        }

    def api_json(self):

        return {
            'id': self.id,
            'code': self.code,
            'description': self.description,
            'status': self.status,
            'created_at': str(self.created_at),
            'updated_at': str(self.updated_at) if self.updated_at else None,
        }

    def save_to_db(self):
        db.session.add(self)
        db.session.commit()

    @classmethod
    def find_by_id(cls, id):
            return cls.query.filter_by(id=id).first()

    @classmethod
    def find_by_code(cls, code):
            return cls.query.filter_by(code=code).first()

    @classmethod
    def find_all(cls, _args):
        search = _args.get('search', '')
        return cls.query.filter(db.func.concat(cls.code, cls.description).like(f"%{search}%"), db.func.length(cls.code) == 23).all()

    def update(self, new_outlet):
        self.code = new_outlet.code 
        self.description = new_outlet.description
        self.status = new_outlet.status
        self.updated_at = datetime.now()
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()

    # generates a new code based on the latest variety code for this product
    def generate_new_code(self):

        # get the last variation code for that specified variety type
        last_variety =  CV.CollectorVarietyModel.query.filter(CV.CollectorVarietyModel.product_id == self.id).order_by(db.desc( db.func.length(CV.CollectorVarietyModel.code) )).order_by(db.desc( CV.CollectorVarietyModel.code )).first()

        print("LAST VARIETY ", last_variety)

        # product code  
        new_code = self.code
        new_suffix = ""

        # check if any was return 
        if last_variety:

            # get the last chars of the code
            last_suffix = last_variety.code[23:]

            # check if the last suffix exist
            if  last_suffix and valid_suffix(last_suffix) :

                # found flag and new suffix
                found = False

                # increment the last suffix by looping though all possible combinations
                for suffix in iter_all_suffixes():

                    # check if the suffix does not exist since len has been passed
                    if suffix == "zzz":
                        break
                    
                    # break if the new suffix has been captured
                    if found and new_suffix:
                        break

                    # if the last suffix has been found then this is the next suffix
                    if found:
                        new_suffix = suffix

                    
                    # check if the last suffix exist and turn On the flag
                    if suffix == last_suffix:
                        found = True
                    
                new_code =  new_code + new_suffix

            elif last_suffix == '':
                new_code = new_code + 'a'
                
            print("NEW CODE: ",new_code)

        return new_code


def iter_all_suffixes():
    for size in itertools.count(1):
        for s in itertools.product(ascii_lowercase, repeat=size):
            yield "".join(s)

def valid_suffix(suffix):

    for char in suffix:
        if char not in alphabetic_letters:
            return False
    return True

alphabetic_letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']