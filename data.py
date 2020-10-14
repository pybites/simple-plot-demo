import requests
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Sequence, Boolean

DATABASE = "sqlite:///stats.db"
ENGINE = create_engine(DATABASE)
STATS_URL = "https://codechalleng.es/api/bites_stats/"

Base = declarative_base()

Session = sessionmaker()
Session.configure(bind=ENGINE)
session = Session()


class Stat(Base):
    __tablename__ = 'bite_stats'
    id = Column(Integer, Sequence('stat_id_seq'), primary_key=True)
    bite = Column(Integer)
    rated = Column(Integer)
    cheated = Column(Boolean)
    submits = Column(Integer)

    def __repr__(self):
        return (f"<Stat(id={self.id}, bite={self.bite}, rated={self.rated}, "
                f"cheated={self.cheated}, submits={self.submits})>")


def insert_data(data):
    stats = []
    for row in data:
        stats.append(Stat(**row))
    session.bulk_save_objects(stats)
    session.commit()


if __name__ == '__main__':
    print("Creating bite_stats table if it does not exist yet")
    Base.metadata.create_all(ENGINE)

    resp = requests.get(STATS_URL)
    resp.raise_for_status()
    data = resp.json()

    print(f"Inserting {len(data)} rows into the DB")
    insert_data(data)
