import csv
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session
from sqlalchemy.exc import IntegrityError
from download import *

engine = create_engine(f'sqlite:///{db_path}', echo=False)
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
session = Session()
Base = declarative_base()


class Listing(Base):
    __tablename__ = 'japan_postal_code'
    id = Column(Integer, primary_key=True, nullable=False)
    zipcode = Column(String(10))
    state = Column(String(255))
    city = Column(String(255))
    address = Column(String(255))

    def full_address(self):
        return f"{self.state}{self.city}{self.address}"

    def to_dict(self):
        return {
            'id': self.id,
            'zipcode': self.zipcode,
            'state': self.state,
            'city': self.city,
            'address': self.full_address()
        }


def create_db(url, query_parameters):
    Base.metadata.drop_all(engine)
    print("Clear previous data...")
    Base.metadata.create_all(engine)

    def prepare_listing(row, id):
        return Listing(id=id, zipcode=row[2].zfill(7), state=row[6], city=row[7], address=row[8])

    name = download_zip(url, query_parameters)
    print("Downloading zip file...")

    with open(f"{name}", encoding='utf-8', newline='') as csv_file:
        csvreader = csv.reader(csv_file, quotechar='"')
        print("Reading csv file...")
        for index, row in enumerate(csvreader, start=1):
            if len(row) > 8:  # Ensure the row has enough elements
                try:
                    listing = prepare_listing(row, index)
                    session.add(listing)
                    session.commit()  # Commit inside the loop to catch exceptions per row
                except IntegrityError:
                    session.rollback()  # Rollback the session to a clean state
                    print(f"Skipping duplicate id: {index}")

    print("DB READY")
    return name
