"""Database creation for ITEC649 Python application"""

DATABASE_NAME = "itec649.db"


def create_tables(db):
    """Create and initialise the database tables
    This will have the effect of overwriting any existing
    data."""

    sql = """
DROP TABLE IF EXISTS people;
CREATE TABLE people (
  id integer unique primary key,
  first_name text,
  middle_name text,
  last_name text,
  email text,
  phone text
);

DROP TABLE IF EXISTS companies;
CREATE TABLE companies (
  id integer unique primary key,
  name text,
  url text,
  contact integer,
  FOREIGN KEY(contact) REFERENCES people(id)
);

DROP TABLE IF EXISTS positions;
CREATE TABLE positions (
  id integer unique primary key autoincrement,
  title text,
  location text,
  company integer,
  FOREIGN KEY(company) REFERENCES companies(id)
);"""

    db.executescript(sql)
    db.commit()


if __name__=='__main__':
    import sqlite3
    # if we call this script directly, create the database and make sample data
    db = sqlite3.connect(DATABASE_NAME)
    create_tables(db)