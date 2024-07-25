import os
import pandas
import ibm_db
import ibm_db_dbi
import datetime
import IPython
import saspy

from saspy import SASsession

class Db2BB:
    r"""
    Clase que trata dos db2
    """
    
    def conector_db2(uid: str, pwd: str, host: str, port: str, database: str):
        """        
        """
        con = ibm_db.connect("DATABASE="+database+";HOSTNAME="+host+";PORT="+port+";PROTOCOL=TCPIP;UID="+uid+";PWD="+pwd+";", "", "")
        conn = ibm_db_dbi.Connection(con)
        return con, conn
    
        
    def select_db2(uid: str, pwd: int, sql: str):
        """        
        """
        query = f"({sql})"

        conn = ibm_db.connect(f'DATABASE=BDB2P04;HOSTNAME=gwdb2.bb.com.br;PORT=50100;PROTOCOL=TCPIP;'f'UID={uid};'f'PWD={pwd};' , '', '')

        dbi = ibm_db_dbi.Connection(conn)

        df = pandas.read_sql(query, dbi)

        return df
        
        ibm_db.close(dbi)
        
    
    def insert_db2(uid: str, pwd: int, df: pandas.DataFrame, table: str, truncate: bool=False):
        """
        Insere dados de um DataFrame pandas em uma tabela DB2.
        """
        
        try:
            conn = ibm_db.connect(f'DATABASE=BDB2P04;HOSTNAME=gwdb2.bb.com.br;PORT=50100;PROTOCOL=TCPIP;'f'UID={uid};'f'PWD={pwd};' , '', '')
            print("Conexão criada.")

            if truncate:
                truncate_query = f"TRUNCATE TABLE {table}"
                stmt = ibm_db.exec_immediate(conn, truncate_query)
                print(f"Tabela {table} truncada.")

            if isinstance(df, pandas.DataFrame):
                columns = ', '.join(df.columns)
                placeholders = ', '.join(['?' for _ in df.columns])
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"       
                print(query)

                for row in df.itertuples(index=False, name=None):
                    stmt = ibm_db.prepare(conn, query)
                    ibm_db.execute(stmt, row)

                print("Dados inseridos com sucesso a partir do DataFrame.")
            elif isinstance(df, dict):
                columns = ', '.join(df.keys())
                placeholders = ', '.join(['?' for _ in df.keys()])
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                stmt = ibm_db.prepare(conn, query)
                ibm_db.execute(stmt, tuple(df.values()))
                print("Registo único inserido com sucesso.")
            else:
                print("Tipo de dados inválido. Forneça um dicionário ou um DataFrame pandas.")

            ibm_db.close(conn)
            print("Conexão fechada.")
        except Exception as e:
            print(f"Erro: {e}")
    

class PandasBB:    
    r"""Classe criada para auxiliar leitura, cargas e validações de dados do command center
    
    Métodos
    ----------
    select_db2: 
    insert_db2:
    check_nulls: 
    check_datatypes: 
    check_max_value: 
    check_duplicates: 
    """
    
    def check_nulls(df: pandas.DataFrame, columns: list):
        """
        """
        n = 0
        for col in columns:
            if df[col].isnull().any():
                print(f"Coluna {col} possui valores nulos")
                n = n+1
        print(f"{n} colunas com valores nulos...\n")
        
        
    def check_datatypes(df: pandas.DataFrame, columns: list):
        """
        """
        for col in columns:
            if df[col].dtype == 'object':
                print(f'{col}: String')
            elif df[col].dtype in [np.int64, np.float64]:
                print(f'{col}: Number')
            elif df[col].dtype == 'bool':
                print(f'{col}: Boolean')
        
                
    def check_max_value(series):
        """
        Example call:
            maxlen = df.apply(check_max_value)
        """
        return series.apply(lambda x: len(str(x))).max()
    
    
    def check_duplicates(df: pandas.DataFrame, key: list) -> pandas.DataFrame:
        """
        """
        if key_columns:
            duplicates = df[df.duplicated(subset=key, keep=False)]
        else:
            duplicates = df[df.duplicated(keep=False)]
            
        print(f"Existem {len(duplicates.index)} registros duplicados")
        
        return duplicates
    
    def order_columns(df_pre: pandas.DataFrame, df_pos: pandas.DataFrame) -> pandas.DataFrame:
        """
        """
        df = df_pre[df_pos.columns]
        
        return df
    
    
    def rename_column(df: pandas.DataFrame, column: str, renamed: str) -> pandas.DataFrame:
        """
        """
        df = df.rename(columns={f'{column}': f'{renamed}'})
        
        return df
    
    
    def percent_variation(df: pandas.DataFrame, column: str, expected: int):
        """
        """
        # Calculate percent variation
        df['VAR'] = df[f"{column}"].pct_change() * 100
        
        # Filter rows where percent_variation > expected
        outlier = df[df['VAR'] > expected]
        
        return outlier
    
    
    def max_date_exists(df1: pandas.DataFrame, df2: pandas.DataFrame, column: str):
        """
        """
        # Get the maximum date from df1
        max_date = df1[column].max()

        # Check if max_date_df1 exists in df2
        exists = max_date in df2[column].values

        return exists
    
    
    def count_by_group(df: pandas.DataFrame, column: str):
        """
        """
        grouped = df.groupby(column).size().reset_index(name='count')
        g = len(grouped.index)
        
        return g
    
    
    def get_timestamp(df: pandas.DataFrame) -> pandas.DataFrame:
        """
        """
        df = df.assign(timestamp=pandas.to_datetime(datetime.datetime.now()))
        
        return df     
    
    
    def set_column_types(df: pandas.DataFrame) -> pandas.DataFrame:
        """
        """
        for column in df.columns:
            if column.startswith('CD') or column.startswith('QT'):
                df[column] = df[column].astype('int64')
            elif column.startswith('NM') or column.startswith('TX'):
                df[column] = df[column].astype('str')
            elif column.startswith('VL'):
                df[column] = df[column].astype('float')
            elif column.startswith('DT'):
                df[column] = pd.to_datetime(df[column], unit='s')
                df[column] = df[column].dt.strftime('%Y-%m-%d')
            elif column.startswith('TS'):
                df[column] = pd.to_timestamp(df[column], unit='s')
                df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%s')
        return df
    
    
    def fill_na(df: pandas.DataFrame) -> pandas.DataFrame:
        """
        """
        for column in df.columns:
            if df[column].dtype == 'int64' or df[column].dtype == 'float64':
                df[column] = df[column].fillna(value=-8)
            elif df[column].dtype == 'object':
                df[column] = df[column].fillna(value=' ')
        return df
    
        
class SparkBB():
    """
    """

    # Função para gravar tabelas no DB2
    def insert_jdbc(df, table: str, trunc: bool=False):    
        db2url = "jdbc:db2://gwdb2.bb.com.br:50100/BDB2P04"
        db2driver = "com.ibm.db2.jcc.DB2Driver" 

        if trunc:
            trunc = "true"
            mode = "overwrite"
        else: 
            trunc = "false"
            mode = "append"

        df.write \
            .format("jdbc") \
            .option("url", db2url) \
            .option("driver", db2driver) \
            .option("user", db2user) \
            .option("password", db2pwd) \
            .option("dbtable", table)\
            .option("truncate", trunc)\
            .mode(mode)\
            .save()


class SasBB():
    """
    """
    
    def get_sas_session() -> SASsession:
        """ Retorna a sessão SAS utilizada no kernel do notebook """
        
        kernel = IPython.Application.instance().kernel
        sas_session = kernel.mva
        
        return sas_session
    
    def list_libnames(session) -> list:
        """ Submit SAS code to get the list of all libnames """
        
        code = """
        proc datasets library=_all_ nolist;
        run;
        quit;
        """
        # Submit the code and capture the log
        results = session.submit(code, 'text')

        # Parse the log to extract libnames
        log = results['LOG']
        libnames = set()

        for line in log.split('\n'):
            if 'Library' in line and 'Contents' in line:
                libname = line.split()[1]
                libnames.add(libname)

        return list(libnames)
    
    def list_tables(session, libname: str='WORK'):
        """  """
        code = f"""
        proc sql;
            select memname from dictionary.tables
            where libname = '{libname}';
        quit;
        """

        result = session.submit(code, results='text')
        lines = result['LST'].split('\n')
        tables = []
        for line in lines:
            if line.strip() and not line.startswith(('NOTE:', 'ERROR:', 'WARNING:')):
                tables.append(line.strip())

        return tables
    
    def list_files(session, path: str):
        """  """
        dl = session.dirlist(path)
        print("Directories under "+path)
        for i in dl:
            if i[len(i)-1:] == '/':
                print(i)
                
        print("\nFiles under "+path)
        for i in dl:
            if i[len(i)-1:] != '/':
                print(i)
    
    def select_sas(session, table: str, libref: str) -> pandas.DataFrame:
        """  """
        
        try:
            df = session.sd2df(table=f"{table}", libref=f"{libref}")
            print(f"{libref}.{table}"+"importado para o Python com {} linhas e {} colunas".format(*df.shape))
        except Exception as e:
            print(f"Erro: {e}")
        
        return df