# data_base_connection imports
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc
import sys
# get_pandas_from_query imports
import time
import sys
import pandas as pd
from sqlalchemy import exc
# save_table imports
import pandas as pd
import sys
from sqlalchemy import exc


def data_base_connection(username, password, host, bd_name):
    """ Use identifiers to connect to the database.
    Args:
        username (str) : user name
        password (str) : user password
        host (str) : data base host name
        bd_name (str) : data base name
    Returns:
        sqlalchemy.session: data base session, use to make request
        sqlalchemy.engine: data base engine, used by session to know which data base is linked
    """

    try:
        # Connect database
        db_string = "postgresql://{}:{}@{}/{}".format(username, password, host, bd_name)
        engine = db.create_engine(db_string)

        # Session opening
        func_session = sessionmaker(bind=engine)
        session = func_session()
        return session, engine
        
    except exc.OperationalError:
        print('An exception flew by!')
        print("The connection with the data table failed - check your permissions")
        sys.exit(1)


def get_pandas_from_query(query, class_name, verbose=True):
    """ Read the data table with the query and return a dataframe with the answer.
    Args:
        query (str) : SQL instruction query
        class_name (str) : class name to indicate the object of the error
        verbose (bool) : True to print the number of line downloaded and time
    Returns:
        pd.DataFrame: DataFrame with query result
    """

    try:
        start = time.time()
        df = pd.read_sql(query.statement, query.session.bind)
        end = time.time()
        # write the time taken to execute the query
        if verbose:
            if (end - start) < 60:
                print("Execution time to get the data from {}: {:.2f} secondes"\
.format(class_name, (end - start)))
            else:
                print("Execution time to get the data from {}: {:.2f} minutes"\
.format(class_name, (end - start) / 60))
            print("Number of line(s) loaded for {} : {} ".format(class_name, len(df)))
        return df

    except exc.OperationalError:
        print('An exception flew by!')
        print('The model for the class {} is not good, or the query is invalid : \n{}'\
.format(class_name, query))
        sys.exit(1)


def save_table(df_res, dtype_res, table_name, engine):
    """ Save the new data collected into the table specifiedin parameter
    Args:
        df_res (pd.DataFrame): DataFrame with the resulting columns
        dtype_res (dict): Dictionary of column names with db type corresponding
        table_name (str): Table name where to store the DataFrame values
        engine (sqlalchemy.engine): data base engine, used by session to know which data base is linked
    """

    if len(df_res) != 0:
        try:
            # Insert in table
            df_res.to_sql(table_name, engine, if_exists='append', schema='public', index=False,
                          chunksize=500, dtype=dtype_res, method="multi")
            print("Rows added: {}".format(len(df_res)))
        except AttributeError:
            print('An exception flew by!')
            print('The attributes for the class {} are not good'.format(class_name))
            sys.exit(1)     
        except exc.OperationalError:
            print('An exception flew by!')
            print("The connection with the data table failed - check your permissions")
            sys.exit(1)
