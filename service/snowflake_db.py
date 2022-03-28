from snowflake.connector import connect
from service import SNOWFLAKE_ACCOUNT, SNOWFLAKE_PW, SNOWFLAKE_USER
from common.logs import setup_logging, log_msg

LOG = setup_logging(__name__)


class connection:
    def __init__(self, user=SNOWFLAKE_USER, password=SNOWFLAKE_PW, account=SNOWFLAKE_ACCOUNT):
        self.user = user
        self.password = password
        self.account = account


    def __enter__(self):
        self.connect_to_snowflake(self.user, self.password, self.account)
        return self


    def __exit__(self, *args):
        self.con.close()
        log_msg(LOG, f'Closed connection to snowflake account {self.account}')


    def connect_to_snowflake(self, user, password, account):
        self.con = connect(user=user, password=password, account=account)
        log_msg(LOG, f'Successfully connected to snowflake account {account}')


    def execute_sql(self, query, verbose=True):
        cur = self.con.cursor()
        log_msg(LOG, f'Executing query:\n {query}', verbose=verbose)

        cur.execute(query)
        log_msg(LOG, f'Query id {cur.sfqid} ran successfully')

        return cur


    def execute_sql_return_df(self, query, verbose=True):
        cur = self.con.cursor()
        log_msg(LOG, f'Executing query:\n {query}', verbose=verbose)

        cur.execute(query)
        log_msg(LOG, f'Query id {cur.sfqid} ran successfully')
        df = cur.fetch_pandas_all()

        return df


    def execute_sql_return_column_values(self, query, column, verbose=True):
        df = self.execute_sql_return_df(query, verbose)
        lowercase_columns = {x:x.lower() for x in df.columns}
        lowercase_search_column = column.lower()
        df.rename(columns=lowercase_columns,inplace=True)

        values = df[lowercase_search_column].values.tolist()
        return values

        
    def yield_query_results(self, query, verbose=True):
        cur = self.execute_sql(query, verbose)
        return cur.fetch_pandas_batches()