# Class packages
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean
Base = declarative_base()
# Queries packages
from sqlalchemy import or_, and_, func, distinct
import sys


class Company(Base):
    __tablename__ = 'company'
    id_company = Column("id_company", Integer, primary_key=True)
    id_naic_sector = Column("id_naic_sector", Integer)
    id_finnhub_classification = Column("id_finnhub_classification", Integer)
    id_gics_sector = Column("id_gics_sector", Integer)


class CompanyDescription(Base):
    __tablename__ = 'companydescription'
    id_description = Column("id_description", Integer, primary_key=True)
    name = Column("name", String)
    description = Column("description", String)
    date_description = Column("date_description", String)
    id_company = Column("id_company", Integer)


class CompanyContact(Base):
    __tablename__ = 'companycontact'
    id_contact = Column("id_contact", Integer, primary_key=True)
    weburl = Column("weburl", String)
    logo = Column("logo", String)
    phone = Column("phone", String)
    date_contact = Column("date_contact", String)
    id_company = Column("id_company", Integer)
    

class CompanyIpo(Base):
    __tablename__ = 'companyipo'
    id_ipo = Column("id_ipo", Integer, primary_key=True)
    number_shares = Column("number_shares", Integer)
    avg_share_price = Column("avg_share_price", Float)
    date_ipo = Column("date_ipo", String)
    id_company = Column("id_company", Integer)
    

def get_query_company(session):
    """ Get all the values in company data table
    Args:
        session (sqlalchemy.session): data base session, use to make request
    Return:
        str: string conataining the sql request to send to postgres
    """

    # ---  Set queries ---
    class_name = "Company"
    try:
        # Get Company details
        query = session.query(Company.id_company)
        return query

    except AttributeError:
        print('An exception flew by!')
        print('The attributes for the class {} are not good, or the query\
code syntax has an error'.format(class_name))
        sys.exit(1)

        
def get_query_company_ipo(session):
    """ Get all the values in CompanyIpo data table
    Args:
        session (sqlalchemy.session): data base session, use to make request
    Return:
        str: string conataining the sql request to send to postgres
    """

    # ---  Set queries ---
    class_name = "CompanyIpo"
    try:
        # Get Company details
        query = session.query(CompanyIpo.id_ipo,
                              CompanyIpo.date_ipo,
                              CompanyIpo.id_company)
        return query

    except AttributeError:
        print('An exception flew by!')
        print('The attributes for the class {} are not good, or the query\
code syntax has an error'.format(class_name))
        sys.exit(1)
        
        
def get_query_company_contact(session):
    """ Get all the values in CompanyContact data table
    Args:
        session (sqlalchemy.session): data base session, use to make request
    Return:
        str: string conataining the sql request to send to postgres
    """

    # ---  Set queries ---
    class_name = "CompanyContact"
    try:
        # Get Company details
        query = session.query(CompanyContact.id_contact,
                              CompanyContact.logo,
                              CompanyContact.weburl,
                              CompanyContact.phone,
                              CompanyContact.date_contact,
                              CompanyContact.id_company)
        return query

    except AttributeError:
        print('An exception flew by!')
        print('The attributes for the class {} are not good, or the query\
code syntax has an error'.format(class_name))
        sys.exit(1)
        
        
def get_query_company_description(session):
    """ Get all the values in CompanyDescription data table
    Args:
        session (sqlalchemy.session): data base session, use to make request
    Return:
        str: string conataining the sql request to send to postgres
    """

    # ---  Set queries ---
    class_name = "CompanyDescription"
    try:
        # Get Company details
        query = session.query(CompanyDescription.id_description,
                              CompanyDescription.name,
                              CompanyDescription.date_description,
                              CompanyDescription.id_company)
        return query

    except AttributeError:
        print('An exception flew by!')
        print('The attributes for the class {} are not good, or the query\
code syntax has an error'.format(class_name))
        sys.exit(1)
