# from db import db
# from models.area import AreaModel

# class OutletModel(db.Model):

#     __tablename__ = 'collector_outlet'

#     id = db.Column(db.Integer, primary_key=True)
#     est_name = db.Column(db.String(80), nullable=False)
#     address = db.Column(db.String(80), nullable=False)
#     phone = db.Column(db.Integer, nullable=True)
#     area_id = db.Column(db.Integer, db.ForeignKey("area.id"), nullable=True)
    

#     def __init__(self, est_name, address, phone, area_id):
#         self.est_name = est_name
#         self.address = address
#         self.phone = phone
#         self.area_id = area_id
        

#     def __str__(self):
#         return str(self.json())


#     def json(self):
#         return {
#             'id': self.id,
#             'est_name': self.est_name,
#             'address': self.address,
#             'phone': self.phone,
#             'area_id': self.area_id,
#             "area": self.area.json() if self.area else None,
#         }

#     def save_to_db(self):
#         print(self)
#         db.session.add(self)
#         db.session.commit()

#     @classmethod
#     def find_by_id(cls, id):
#             return cls.query.filter_by(id=id).first()

#     @classmethod
#     def find_all(cls):
#         return cls.query.all()

#     def update(self, new_outlet):
#         self.est_name = new_outlet.est_name
#         self.address = new_outlet.address
#         self.phone = new_outlet.phone
#         self.area_id = new_outlet.area_id
#         db.session.commit()

#     def delete(self):
#         db.session.delete(self)
#         db.session.commit()