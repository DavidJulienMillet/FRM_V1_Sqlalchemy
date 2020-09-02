import pandas as pd

from datetime import date
import time
import re

import sys
from os import listdir
from os.path import isfile, join

from connections import *
from model_requests import *


def read_csv_files(mypath):
    """ Read all the csv got from finnhub. 
    The file names should be "company_profile2_0_499.csv", "company_profile2_500_999.csv"... 
    The data are split in many files because each set of 500 companies takes 3 hours to request.
    It is only a precaution for crases, they have the same structure. 
    Args:
        mypath (str) : Location of the files folder
    Returns:
        pd.DataFrame: Table with an union of all files, cleaned for empty and duplicate lines. 
    """

    # Read all the csv to know if there is a job to continue for this program
    lst_files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    
    # Read all the input csv
    df_company_profile2 = pd.DataFrame()
    for file in lst_files:
        # Check if the file is a good one
        expression = "^company_profile2_[0-9]*_[0-9]*.csv$"
        if re.search(expression, file):
            df_company_profile2 = df_company_profile2\
            .append(pd.read_csv(mypath + file, dtype=object))
    print("Number of rows loaded: ", len(df_company_profile2))
    
    # Drop nan lines
    df_company_profile2.dropna(subset=['ticker'], inplace=True)
    print("Number of rows kept after ticker dropna: ", len(df_company_profile2))
    
    # Drop duplicates
    if "date_description" not in df_company_profile2.columns:
        df_company_profile2["date_description"] = date.today()
    columns = ["name", "logo", "weburl", "phone", "ipo", "date_description"]
    df_company_profile2 = df_company_profile2.drop_duplicates(subset="name", keep="first")
    df_company_profile2 = df_company_profile2[columns]
    df_company_profile2.reset_index(inplace=True, drop=True)
    print("Number of rows kept after drop comp name duplicates: ", len(df_company_profile2))
    
    return df_company_profile2

    
def read_data_base(session):
    """ Load and merge tables Company, CompanyContact, CompanyIpo and CompanyDescription
    Args:
        session (sqlalchemy.session): data base session, use to make request
    Returns:
        pd.DataFrame: Table with the 4 data table joined on id_company
    """

    # load tables
    query = get_query_company(session)
    df_comp_base = get_pandas_from_query(query, "Company", verbose=False)
    
    query = get_query_company_ipo(session)
    df_comp_ipo_base = get_pandas_from_query(query, "CompanyIpo", verbose=False)

    query = get_query_company_description(session)
    df_comp_desc_base = get_pandas_from_query(query, "CompanyDescription", verbose=False)

    query = get_query_company_contact(session)
    df_comp_contact_base = get_pandas_from_query(query, "CompanyContact", verbose=False)
    
    # Merge the data tables 
    df_merged_base =  pd.merge(df_comp_base, df_comp_ipo_base, 
                               on='id_company', how='left')
    
    df_merged_base =  pd.merge(df_merged_base, df_comp_contact_base, 
                           on='id_company', how='left')
    
    df_merged_base =  pd.merge(df_merged_base, df_comp_desc_base, 
                           on='id_company', how='left')
    
    return df_merged_base


def keep_new_names(df_company_profile2, df_merged_base, verbose=True):
    """ keep names which are not already in the database
    If the database is empty, the date where the data was get from finnuhub is removed.
    If the database is not empty, that means it is new record and the date has a meaning.
    Args:
        df_company_profile2 (pd.Dataframe): New data to check, table got from the files
        df_merged_base (pd.Dataframe): Data from the database
        verbose (bool): Print extra infos if True
    Returns:
        pd.DataFrame: Table filtered with only new names.
    """

    # Check if there is new names from finnhub
    df_company_new_name = pd.merge(df_company_profile2, df_merged_base[['name']], 
                                   on=['name'], how='left', indicator='Exist')
    df_company_new_name = df_company_new_name.loc[df_company_new_name["Exist"] == "left_only"]
    df_company_new_name.drop("Exist", inplace=True, axis=1) 
    df_company_new_name.reset_index(inplace=True, drop=True)
    if verbose:
        print("Number of rows kept after check names already in table: ", len(df_company_new_name))
    
    # Delete the date if it is the first time the code run, the date is not the date of a changing
    if df_merged_base.empty:
        df_company_new_name["date_description"] = None
        
    return df_company_new_name


def check_status_new_name(df_company_new_name, df_merged_base, verbose=True):
    """ From the record with new names, separate the rows where the name refers to a 
    new company from the rows where the new name refers to a company changing name.
    It is a company changing name if the date Ipo is the same than an existing one and 
    either the phone number or the weburl is the same than the existing one.
    if the name has changed, store the id_company of the corresponding company.
    Args:
        df_company_new_name (pd.Dataframe): Table filtered with only new names.
        df_merged_base (pd.Dataframe): Data from the database
        verbose (bool): Print extra infos if True
    Returns:
        pd.DataFrame: Table with new companies
        pd.DataFrame: Table where names has changed from exiting companies
    """

    # check if the name could have changed
    start = time.time()

    columns = ['id_company', 'logo', 'weburl', 'phone', 'ipo', 'date_description', 'name']
    df_comp_to_add = pd.DataFrame(columns=columns)
    df_comp_names_changed = pd.DataFrame(columns=columns)

    for ind in df_company_new_name.index:
        name = df_company_new_name.loc[ind, ["name"]].values[0]
        phone = df_company_new_name.loc[ind, ["phone"]].values[0]
        weburl = df_company_new_name.loc[ind, ["weburl"]].values[0]
        ipo = df_company_new_name.loc[ind, ["ipo"]].values[0]

        # changed if url or phone is the same, and IPO same
        df_query = df_merged_base.query('@ipo == date_ipo and (@phone == phone or @weburl == weburl)')
        
        if df_query.empty:
            df_comp_to_add = df_comp_to_add.append(df_company_new_name.loc[ind])
        else:
            df_query.reset_index(inplace=True, drop=True)
            id_company = df_query.loc[0, ["id_company"]].values[0]
            df_company_new_name.at[ind, "id_company"] = id_company
            df_comp_names_changed = df_comp_names_changed.append(df_company_new_name.loc[ind])

    end = time.time()
    
    if verbose:
        if (end - start) < 60:
            print("Execution time {:.2f} secondes".format(end - start))
        else:
            print("Execution time {:.2f} minutes".format((end - start) / 60))
        print("Number of line(s) treated : {}".format(len(df_company_new_name)))
            
    return df_comp_to_add, df_comp_names_changed



def keep_new_contacts(df_company_profile2, df_merged_base, verbose=True):
    """ keep contacts which are not already in the database
    If the database is empty, the date where the data was get from finnuhub is removed.
    If the database is not empty, that means it is new record and the date has a meaning.
    Args:
        df_company_profile2 (pd.Dataframe): New data to check, table got from the files
        df_merged_base (pd.Dataframe): Data from the database
        verbose (bool): Print extra infos if True
    Returns:
        pd.DataFrame: Table filtered with only new names.
    """

    # Check if there is new names from finnhub
    df_company_new_contact = pd.merge(df_company_profile2, df_merged_base[['phone', 'weburl', 'logo']], 
                                      on=['phone', 'weburl', 'logo'], how='left', indicator='Exist')
    df_company_new_contact = df_company_new_contact.loc[df_company_new_contact["Exist"] == "left_only"]
    df_company_new_contact.drop("Exist", inplace=True, axis=1) 
    df_company_new_contact.reset_index(inplace=True, drop=True)
    if verbose:
        print("Number of rows kept after check contacts already in table: ", len(df_company_new_contact))
    
    # Delete the date if it is the first time the code run, the date is not the date of a changing
    if df_merged_base.empty:
        df_company_new_contact["date_description"] = None
        
    return df_company_new_contact


def check_status_new_contact(df_company_new_contact, df_merged_base, verbose=True):
    """ From the record with new contacts, separate the rows where the change refers to a 
    new company from the rows where the change refers to a company changing concacts.
    It is a company changing contact if (the name exists) or ((the date Ipo is the same of 
    existing one) and (either the phone number or the weburl is the same than the existing one)).
    If the contact has changed, store the id_company of the corresponding company.
    Args:
        df_company_new_contact (pd.Dataframe): Table filtered with only new contacts.
        df_merged_base (pd.Dataframe): Data from the database
        verbose (bool): Print extra infos if True
    Returns:
        pd.DataFrame: Table with new companies
        pd.DataFrame: Table where contacts has changed from exiting companies
    """

    # check if the contact could have changed
    start = time.time()

    columns = ['id_company', 'logo', 'weburl', 'phone', 'ipo', 'date_description', 'name']
    df_comp_to_add = pd.DataFrame(columns=columns)
    df_comp_contact_changed = pd.DataFrame(columns=columns)

    for ind in df_company_new_contact.index:
        name = df_company_new_contact.loc[ind, ["name"]].values[0]
        phone = df_company_new_contact.loc[ind, ["phone"]].values[0]
        weburl = df_company_new_contact.loc[ind, ["weburl"]].values[0]
        ipo = df_company_new_contact.loc[ind, ["ipo"]].values[0]

        # changed if url or phone is the same, and IPO same
        condition = '(@ipo == date_ipo and (@phone == phone or @weburl == weburl)) or (@name == name and @ipo == date_ipo)'
        df_query = df_merged_base.query(condition)

        if df_query.empty:
            df_comp_to_add = df_comp_to_add.append(df_company_new_contact.loc[ind])
        else:
            df_query.reset_index(inplace=True, drop=True)
            id_company = df_query.loc[0, ["id_company"]].values[0]
            df_company_new_contact.at[ind, "id_company"] = id_company
            df_comp_contact_changed = df_comp_contact_changed.append(df_company_new_contact.loc[ind])

    end = time.time()
    
    if verbose:
        if (end - start) < 60:
            print("Execution time {:.2f} secondes".format(end - start))
        else:
            print("Execution time {:.2f} minutes".format((end - start) / 60))
        print("Number of line(s) treated : {}".format(len(df_company_new_contact)))
            
    return df_comp_to_add, df_comp_contact_changed


def set_company_id(df_comp_to_add, df_merged_base):
    """ Associate a company id to new companies. Get the highest value of id from database.
    Args:
        df_comp_to_add (pd.Dataframe): Table with new companies
        df_merged_base (pd.Dataframe): Data from the database
    Returns:
        pd.DataFrame: Table with new companies,with the column id_company filled
    """

    # Get all company ids from database to know which are left
    lst_id_comp = df_merged_base["id_company"].values
    if len(lst_id_comp) != 0:
        max_id_company = max(df_merged_base["id_company"].values)
    else:
        max_id_company = 0
        
    # Gives new company id to companies to add
    for ind in df_comp_to_add.index:
        df_comp_to_add.at[ind, "id_company"] = max_id_company + 1
        max_id_company = max_id_company + 1
        
    return df_comp_to_add


def format_data(df_comp_to_add, df_comp_names_changed):
    """ From the names changed and the new companies tables, format the data to 
    match the database tables Company, CompanyDescription, CompanyContact, CompanyIpo
    Args:
        df_comp_to_add (pd.Dataframe): Table with new companies
        df_comp_names_changed (pd.Dataframe): Table where names has changed from exiting companies
    Returns:
        pd.DataFrame: Table Company
        pd.DataFrame: Table CompanyDescription
        pd.DataFrame: Table CompanyContact
        pd.DataFrame: Table CompanyIpo
    """

    # Format dataframe to company table
    df_company = df_comp_to_add["id_company"].copy()

    # Format dataframe to company_description table
    columns = ["date_description", "name", "id_company"]
    df_comp_description = df_comp_to_add[columns].copy()
    df_comp_description = df_comp_description.append(df_comp_names_changed[columns])

    # Format dataframe to company_contact table
    columns = ["date_description", "logo", "phone", "weburl", "id_company"]
    df_comp_contact = pd.DataFrame(columns = columns)
    df_comp_contact = df_comp_to_add[columns].copy()
    df_comp_contact.rename(columns={'date_description': 'date_contact'}, inplace=True)

    # Format dataframe to company_ipo table
    df_comp_ipo = pd.DataFrame(columns=["date_ipo", "id_company"])
    df_comp_ipo[["date_ipo", "id_company"]] = df_comp_to_add[["ipo", "id_company"]].copy()

    return df_company, df_comp_description, df_comp_contact, df_comp_ipo
