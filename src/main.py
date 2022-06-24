import psycopg2 as pg
import pandas as pd
from loguru import logger



class ReadData:
    
    def __init__(self, database, user, password, host, port):
        
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port


    def read_data(self, sql_query):

        try:

            with pg.connect(database=self.database, 
                            user=self.user, 
                            password=self.password, 
                            host=self.host, 
                            port=self.port) as conn:
                
                with conn.cursor() as curs:
                    curs.execute(sql_query)

                    data = curs.fetchall()
                    logger.info(f'data read successfully')
                   

            return data

        except:
            print(f'connection failed')
                       
            
    def get_dataframe(self, column_query, data_query):
        
        data = self.read_data(data_query)
        columns = self.read_data(column_query)
        columns = [i[0] for i in columns]

        try:
            df = pd.DataFrame(data=data, columns=columns)
            logger.info(f'dataframe is created')

            return df

        except Exception as e:
            
            print(f' create dataframe is failed')
            print(e)
            
            
if __name__ == '__main__':
    
    data_obj = ReadData(database="DVD_Rental", 
                        user="andisheh", 
                        password="1234", 
                        host="0.0.0.0", 
                        port=5432)
    
    
    sql_query = 'select * from actor'
    
    column_query = """ SELECT column_name
                    FROM information_schema.columns
                    where table_name = 'actor'
                    order by column_name """
    
    
    df = data_obj.get_dataframe(column_query, sql_query)

