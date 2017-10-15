

import glob
import ujson
import os
import click

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine, event, Column, Integer, String
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base


db_path = os.path.join(os.path.dirname(__file__), 'years.db')
url = URL(drivername='sqlite', database=db_path)
engine = create_engine(url)
factory = sessionmaker(bind=engine)
session = scoped_session(factory)


Base = declarative_base()
Base.query = session.query_property()


class Year(Base):

    __tablename__ = 'year'

    __table_args__ = dict(sqlite_autoincrement=True)

    id = Column(Integer, primary_key=True)

    prefix = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    suffix = Column(String, nullable=False)

    @classmethod
    def load(cls, pattern):
        """Load rows.
        """
        for path in glob.glob(pattern):
            with open(path) as fh:

                segment = [ujson.loads(line) for line in fh]
                session.bulk_insert_mappings(cls, segment)

                session.commit()
                print(path)


@click.group()
def cli():
    pass


@cli.command()
def create():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


@cli.command()
@click.argument('path', type=click.Path())
def load(path):
    Year.load(path)


if __name__ == '__main__':
    cli()
