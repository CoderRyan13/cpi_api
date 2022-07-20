from db import db, portal_db

class CollectorOutletModel(db.Model):

    __tablename__ = 'collector_outlet'

    id = db.Column(db.Integer, primary_key=True)
    cpi_outlet_id = db.Column(db.Integer, unique=True, nullable=True)
    est_name = db.Column(db.String(80), nullable=False)
    address = db.Column(db.String(80), nullable=False)
    lat = db.Column(db.Float, nullable=False)
    long = db.Column(db.Float, nullable=False)
    note = db.Column(db.String(1000), nullable=False)
    phone = db.Column(db.Integer, nullable=True)
    area_id = db.Column(db.Integer, db.ForeignKey("collector_area.id"), nullable=False)
 
    # area = db.relationship("CollectorAreaModel", back_populates="outlets")

    def __init__(self, est_name, address, phone, area_id, lat, _long, note, cpi_outlet_id=None, _id=None, ):

        self.id = _id
        self.cpi_outlet_id = cpi_outlet_id
        self.est_name = est_name
        self.address = address
        self.phone = phone
        self.area_id = area_id
        self.lat = lat
        self.long = _long
        self.note = note
        

    def __str__(self):
        return str(self.json())


    def json(self):
        return {
            'id': self.id,
            'cpi_outlet_id': self.cpi_outlet_id,
            'est_name': self.est_name,
            'lat': self.lat,
            'long': self.long,
            'note': self.note,
            'address': self.address,
            'phone': self.phone,
            'area_id': self.area_id,
            "area_name": self.area.name if self.area else None
        }

    def save_to_db(self):
        db.session.add(self)
        db.session.commit()
        

    @classmethod
    def find_by_id(cls, id):
            return cls.query.filter_by(id=id).first()

    # @classmethod
    # def find_by_id(cls, cpi_outlet_id):
    #         return cls.query.filter_by(cpi_outlet_id=cpi_outlet_id).first()

    @classmethod
    def find_by_cpi_outlet_id(cls, cpi_outlet_id):
        return cls.query.filter_by(cpi_outlet_id=cpi_outlet_id).first()

    @classmethod
    def find_all(cls):
        return cls.query.all()

    def update(self, new_outlet):
        self.est_name = new_outlet.est_name
        self.address = new_outlet.address
        self.phone = new_outlet.phone
        self.area_id = new_outlet.area_id
        self.lat = new_outlet.lat
        self.long = new_outlet.long
        self.note = new_outlet.note
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()

    @classmethod
    def cpi_to_collector_outlet(cls, outlet):
        new_outlet = cls(
            cpi_outlet_id=outlet[0],
            est_name=outlet[1],
            note=outlet[2],
            address=outlet[3],
            lat=outlet[4],
            _long=outlet[5],
            phone=outlet[6],
            area_id=outlet[7]
        )
        
        return new_outlet
    
    @classmethod
    def find_by_collector(cls, collector_id):

        query = """
            SELECT DISTINCT 
                collector_outlet.cpi_outlet_id, 
                est_name, 
                note, 
                address, 
                lat, 
                collector_outlet.long, 
                phone, 
                area_id
            FROM collector_outlet 
            JOIN assignment ON collector_outlet.cpi_outlet_id = assignment.outlet_id
            WHERE assignment.collector_id = %s
            AND collector_outlet.cpi_outlet_id IS NOT NULL
        """
        portal_db.execute(query, (collector_id, ))
        outlets = portal_db.fetchall()

        outlets = [ {
            'id': outlet[0],
            'est_name': outlet[1],
            'note': outlet[2],
            'address': outlet[3],
            'lat': outlet[4],
            'long': outlet[5],
            'phone': outlet[6],
            'area_id': outlet[7]
        } for outlet in outlets ]
        

        return outlets

