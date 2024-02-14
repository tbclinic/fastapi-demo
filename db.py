import csv

from dateutil.parser import parse
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from main import download_zip


def create_db(url, query_parameters):
    engine = create_engine('sqlite:///postalcode.db', echo=True)
    Base = DeclarativeBase()

    class Listing(Base):
        __tablename__ = 'Japan Postal Code'
        id = Column(Integer, primary_key=True, nullable=False)
        state = Column(String(100))
        city = Column(String(100))
        address = Column(String(100))

        def full_address(self):
            return "{self.state}{self.city}{self.address}"

    Base.metadata.create_all(engine)

    session = sessionmaker(bind=engine)

    def parse_none(dt):
        try:
            return parse(dt)
        except:
            return None

    def prepare_listing(row):
        row["last_review"] = parse_none(row["last_review"])
        return Listing(**row)

    name = download_zip(url, query_parameters)

    with open(f"{name}.csv", encoding='utf-8', newline='') as csv_file:
        csvreader = csv.DictReader(csv_file, quotechar='"')

        listings = [prepare_listing(row) for row in csvreader]

        session.add_all(listings)
        session.commit()

    return name
