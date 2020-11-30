#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import os
import psycopg2
import logging
import time
from datetime import datetime

logname = '/Users/baccarclement/Desktop/pipeline.log'
logging.basicConfig(filename=logname,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)


# In[4]:


class PostgreDatabase:
    """A class allowing to connect and search in a Postgre database. This class
    is a context, that should be used in a `with` statement
    This class is initialized exactly as the connect function in `psycopg2`.

    The connection parameters are be specified as a string:

         PostgreDatabase(f"postgres://{POSTGRES_USER}:"
                         f"{POSTGRES_PASSWORD}@"
                         f"{POSTGRES_HOST}:"
                         f"{POSTGRES_PORT}/"
                         f"{POSTGRES_DB}")

    Parameters
    ----------
    dsn : `str`
        The DSN string for connection
    """

    def __init__(self, dsn: str):
        self.conn = psycopg2.connect(dsn=dsn)
        logging.info("Connected. DSN :" + self.conn.dsn)
        self.cur = self.conn.cursor()

    def cursor(self):
        """The cursor
        """
        return self.cur

    def execute(self, query: str, args=None):
        """Execute a query.

        Parameters
        ----------
        query : `str`
            The query

        args : `tuple` or `dict`
            The list of arguments to pass for execution of the query
        """
        self.cur.execute(query, args)

    def commit(self):
        """Commit changes to the database.
        """
        self.conn.commit()

    def fetchall(self):
        """Fetch all the results of the executed query
        """
        return self.cur.fetchall()

    def execute_and_fetchall(self, query: str):
        """Execute the query and fetch all results.
        """
        self.execute(query)
        return self.fetchall()

    def get_dataframe(self, query: str) -> pd.DataFrame:
        """Return results as pandas `DataFrame`.

        Parameters
        ----------
        query: `str`
            The query

        Returns
        -------
        output : `pd.DataFrame`
            Pandas dataframe containing results
        """
        return pd.read_sql(query, self.conn)

    def __enter__(self):
        return self

    def __exit__(self, *excs):
        if self.conn:
            self.conn.close()
            logging.info("Disconnected. DSN :" + self.conn.dsn)


# In[5]:


POSTGRES_USER = "surfriderrootuser@pgdb-plastico-dev"
POSTGRES_PASSWORD = "LePlastiqueCaPique!"
POSTGRES_HOST = "pgdb-plastico-dev.postgres.database.azure.com"
POSTGRES_PORT = '5432'
POSTGRES_DB = "plastico-dev"
conn_string = f"host={POSTGRES_HOST} user={POSTGRES_USER} dbname={POSTGRES_DB} password={POSTGRES_PASSWORD}"

## to store in ENV VAR


# In[6]:


def get_query(filename, value, key):
    """



    """

    f_ = open(f"{PATH_DIR}{filename}", "r").read()

    return f_.replace(key, value)

def replace_variable(query, variable):

    for k,v in variable.items():
        query = query.replace(k, v)

    return query


# In[9]:


def clean_all_tables(campaign_ids):
    """


    """

    schemaname = "bi_temp"
    print(f"REMOVE DATA FO CAMPAIGN {campaign_ids}")
    with PostgreDatabase(conn_string)  as db:

        query = f"""
                        SELECT
                            table_name
                        FROM
                        information_schema.columns
                        WHERE table_schema = '{schemaname}' AND column_name = 'id_ref_campaign_fk'

                """

        table_list = db.get_dataframe(query)

        query_list = table_list["query"] = table_list.table_name.map(lambda x: f""" DELETE  FROM {schemaname}.{x} table_name WHERE id_ref_campaign_fk in ({campaign_ids});""").tolist()
        query_list.append(f""" DELETE  FROM {schemaname}.campaign table_name WHERE id in ({campaign_ids});""")

        for query in query_list:

            db.execute(query)
            db.commit()





def query_launcher(df, step, to_replace, key, unique=False):
    """


    """
    to_replace["monitoring"][str(step)] = list()

    with PostgreDatabase(conn_string)  as db:


        for value in to_replace[key]:

            try:
                if step >= 0:
                    start_time = time.time()

                    variables = dict()
                    filename = df.loc[step].files


                    query = get_query(filename, value, key)
                    query = query.replace("@last_run", to_replace["@last_run"])

                    db.execute(query)
                    db.commit()


                    eta = time.time() - start_time
                    print(f"campaign = {value} --- ETA {eta} seconds ---" )

                    to_replace["monitoring"][str(step)].append(eta)

                else:# si step = -1, on vient juste logger dans table de log un SUCCESS
                    metadata = {"initiated_on": pipeline_start_date,
                                "finished_on": str(datetime.now()),
                                "status": "SUCCESS",
                                "script_version" : script_version,
                                "id_ref_campaign_fk": value,
                                "reason" : "None"
                              }
                    query = update_log_table(metadata)
                    print(query)
                    db.execute(query)
                    db.commit()

            except Exception as e :

                error = str(e).replace("\n", " ").replace("\t", " ").replace('"', "")
                clean_all_tables(value)
                to_replace[key].remove(value)

                metadata = {"initiated_on": pipeline_start_date,
                            "finished_on": str(datetime.now()),
                             "status": "HARD_FAIL",
                             "reason": error,
                             "script_version" : script_version,
                             "failed_step": step,
                            "id_ref_campaign_fk": value
                              }
                query = update_log_table(metadata)
                print(query)
                db.commit()

                db.execute(query)
                db.commit()



    return to_replace


def update_log_table(metadata):

    query = ",".join([f" {k} = '{v}'" for k,v in metadata.items() if k != 'id_ref_campaign_fk' ] )
    query = f"""update logs.bi SET {query} where campaign_id = {metadata["id_ref_campaign_fk"]}"""

    return query

# to_replace = tableau qui contient les id des campaign
# execute le script 6 et renvoie les id des rivieres concernées par les nouvelles campaign
def get_river_ids(to_replace):


    with PostgreDatabase(conn_string)  as db:

        filename = df.loc[6].files

        print(filename)

        query = get_query(filename, to_replace["@campaign_ids"], "@campaign_ids")
        river_ids = db.get_dataframe(query)

        print(river_ids)

    if river_ids.shape[0] > 0:

        river_ids = list(set([f"'{i[0]}'::text" for i in river_ids.values]))


    else:
        river_ids = None

    to_replace["@rivers_id"] = river_ids

    return to_replace


def get_campaign_ids(to_replace):
    """



    """

    with PostgreDatabase(conn_string)  as db:

        campaign_ids = db.get_dataframe(f"""

            select c.id from campaign.campaign  c
            left join logs.bi  bi on bi.campaign_id = c.id
            where (bi.id is null)


                        """).id.tolist()


    if len(campaign_ids) > 0:
        campaign_ids = [f"'{i}'::uuid" for i in campaign_ids]
    else:
        campaign_ids = None

    to_replace["@campaign_ids"] = campaign_ids
    return to_replace



# In[10]:


PATH_DIR="../PycharmProjects/plastic-origin/fillbidatabase/SqlScripts/"


files = [i for i in os.listdir(PATH_DIR) if i.endswith(".sql")]
files = [f for f in files if f != 'trash_for_acrgis.sql']
df = pd.DataFrame(files)
df.columns = ["files"]
df["n"] = df.files.map(lambda x: int(x.split('_')[0]))
df = df.sort_values("n").reset_index(drop=True)
df = df.set_index("n")

pipeline_start_date = str(datetime.now())
LAST_RUN = '2019/01/01'
script_version = 0
to_replace={"@last_run":f"'{LAST_RUN}'::date", "\t": " ", "\n": " ", "bi.": "bi_temp.", 'monitoring': dict()}

to_replace = get_campaign_ids(to_replace)

if to_replace["@campaign_ids"] == None:
    print("stop the pipeline")
else:
    print(len(to_replace["@campaign_ids"]))


# ## Compute data at Campaign level

# In[11]:


steps = [0, 1, 2, 3, 4, 5, 6, -1]

for step in steps:
    print(f"STEP --- {step}")
    to_replace = query_launcher(df, step, to_replace, "@campaign_ids", unique=False)
    print("\n")


# ## Compute data at River level

# In[ ]:


to_replace["@campaign_ids"] = ','.join(to_replace["@campaign_ids"])
to_replace = get_river_ids(to_replace)


# In[ ]:


step = 9
to_replace = query_launcher(df, step, to_replace, "@rivers_id")


# ## ETA analysis

# In[ ]:


del to_replace["monitoring"][str(-1)]


# import numpy as np
# import matplotlib.pyplot as plt
# for k in to_replace["monitoring"].keys():
#     to_replace["monitoring"][k] = np.mean(to_replace["monitoring"][k])
#
# eta = pd.DataFrame(to_replace["monitoring"].items(), columns = ["step", "eta_mean"])
# eta.plot.line(x = "step", y = "eta_mean")
# plt.title("eta avg by step")
# plt.legend("")

# In[ ]:
