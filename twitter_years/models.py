

import glob
import ujson

from peewee import SqliteDatabase, Model, CharField, IntegerField


db = SqliteDatabase('years.db')


class Year(Model):

    class Meta:
        database = db

    prefix = CharField()
    year = IntegerField()
    suffix = CharField()

    @classmethod
    def load(cls, pattern):
        """Load rows.
        """
        for path in glob.glob(pattern):
            with open(path) as fh:

                segment = [ujson.loads(line) for line in fh]

                with db.atomic():
                    cls.insert_many(segment).execute()

                print(path)
