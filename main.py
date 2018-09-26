"""
Module to read data from CSV files and HTML file
to populate an SQL database

ITEC649 2018
"""

import csv
import sqlite3
from database import DATABASE_NAME
from bs4 import BeautifulSoup


def read_csv_file(filename):
    """Read a csv file and return a list of dictionaries.

    :param filename: The file to be read.
    :return: The list of dictionaties.
    """
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        data = []
        for row in reader:
            data.append(row)
    return data

def load_person(c,  person):
    """
    Save a single person to database.
    :param db: The database connection.
    :param person: The person to be loaded
    :param c: Cuursor
    :return: None
    """
    c.execute("INSERT INTO people VALUES (:id, :first_name, :middle_name, :last_name, :email, :phone)",
          {
              'id': person['person_ID'],
              "first_name": person['first'],
              "middle_name": person['middle'],
              "last_name": person['last'],
              "email": person['email'],
              "phone": person['phone']
          })


def load_people(db, c):
    """Insert people data from a csv file to database.

    :param db: An active database connection
    :param c: Cuursor
    :return: None
    """
    people = read_csv_file("people.csv")
    for person in people:
        with db:
            load_person(c, person)


def load_company(c,  company):
    """
    Save a single company to database.
    :param db: The database connection.
    :param person: The person to be loaded
    :param c: Cuursor
    :return: None
    """
    c.execute("INSERT INTO companies VALUES (:id, :name, :url, :contact)",
          {
              "id": company['id'],
              "name": company['company'],
              "url": company['url'],
              "contact": company['contact']
          })

def get_person_by_name(c, name):
    """
    Provide the name from company contact and find the person from people table.
    :param c: Cursor
    :param name: Person name
    :return: Person tuple
    """
    name_list = name.replace(',', '').split(' ')
    first_name = name_list[0]
    middle_name = name_list[1]
    try:
        last_name = name_list[2]
    except IndexError:
        last_name = name_list[1]
        middle_name = ''
    c.execute("SELECT * FROM people WHERE "
              "((((last_name=:first_name OR middle_name=:first_name) OR (first_name=:first_name OR middle_name=:first_name)) AND "
              "((last_name=:last_name OR middle_name=:last_name) OR (first_name=:last_name OR middle_name=:last_name))) AND "
              "((last_name=:middle_name OR middle_name=:middle_name) OR (first_name=:middle_name OR middle_name=:middle_name)))",
              {'first_name': first_name, "middle_name": middle_name, "last_name": last_name})
    obj = c.fetchone()
    return obj


def load_companies(db, c):
    """Insert companies data from a csv file to database.

    :param db: An active database connection
    :param c: Cuursor
    :return: None
    """
    companies = read_csv_file("companies.csv")
    id = 1
    for company in companies:
        company["id"]=id
        with db:
            p = get_person_by_name(c, company['contact'])
            company["contact"] = p[0]
            load_company(c, company)
        id+=1
"""
<div class="card" style="width: 18rem;">
   <div class="card-header">
      <h5 class="card-title">Full Stack Developer</h5>
      <div class="company">Booking.com BV</div>
   </div>
   <div class="card-body">
      <h6 class="card-subtitle mb-2 text-muted">
         <span class="user">@Mandible:</span>
         <span class="timestamp">2018-03-01 16:22:28</span>
      </h6>
      <p class="card-text"> 
      <p>Job Description:</p>
      <p>Booking.com is looking for Full Stack Developers all around the globe!  </p>
      <a class="card-link" href="/positions/50">Read More</a>
      </p>
   </div>
</div>
"""
def process_company_html(raw_html):
    """Process raw html from beautifulsoup

    :param raw_html: Raw soup instance object
    :return: Company Dict
    """
    obj = {
        "title": raw_html.find("h5").text,
        "company": raw_html.find("div", class_="company").text,
        "location": raw_html.find("span", class_="user").text.replace('@', "").replace(':', ''),
        "timestamp": raw_html.find("span", class_="timestamp").text,
        "description": raw_html.p.text.strip().replace('Job Description:\n', '').replace("\nRead More", '')
    }
    return obj

def html_job_reader():
    """Reads the index.htmland returns list of Jaob objects.

    :return:
    """
    with open("index.html") as fp:
        soup = BeautifulSoup(fp, features="html.parser")
    all_companies = soup.find_all("div", class_="card")
    data = []
    for company in all_companies:
        data.append(process_company_html(company))
    return data


def get_company_by_name(c, name):
    """Given a  merchant name, get and return the company from database

    :param c: cursor
    :param name:
    :return: company
    """
    c.execute("SELECT * FROM companies WHERE name=:name", {'name': name})
    obj = c.fetchone()
    return obj

def save_job(db, c, job):
    """Save a job

    :param db: Database connection
    :param c: Cursor
    :return:
    """
    c.execute("INSERT INTO positions VALUES (:id, :title, :location, :company)",
              {
                  **job
              })

def save_jobs(db, c):
    """Reads and saves jobs

    :param db: Database connection
    :param c: Current cursor
    :return: None
    """
    jobs = html_job_reader()
    id = 1
    for job in jobs:
        with db:
            company = get_company_by_name(c, job['company'])
            job['company'] = company[0]
            job['id'] = id
            save_job(db, c, job)
        id +=1

def read_given_fields(c):
    """Read the fields tor generating csv

    :param db: Connection
    :param c: Cursor
    :return:
    """
    # c.execute("""
    #         SELECT companies.id, companies.name, people.email
    #         FROM
    #             companies
    #         INNER JOIN
    #             people
    #         ON
    #             companies.contact=people.id
    #         """)
    # print(c.fetchall())
    c.execute("""
        SELECT positions.title, positions.location, companies.name, people.first_name, people.last_name, people.email
        FROM positions 
        JOIN companies ON positions.company = companies.id
        JOIN people ON companies.contact = people.id
    """)
    data = c.fetchall()
    return data


def save_to_csv(db, c):
    """Fetch from database and save the final report to csv

    :param db:
    :param c:
    :return:
    """
    with db:
        data = read_given_fields(c)

    with open("final.csv", "w", newline='') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Company Name', 'Position Title', 'Company Location', 'Contact FirstName', "Contact LastName", "Contact Email"])
        for job in data:
            writer.writerow(
                [job[2], job[0].replace(',', ''), job[1],
                 job[3], job[4], job[5]])


if __name__=='__main__':
    db = sqlite3.connect(DATABASE_NAME)
    c = db.cursor()
    load_people(db, c)
    load_companies(db, c)
    save_jobs(db, c)
    save_to_csv(db, c)
