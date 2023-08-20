from src.cassandra_connection.connect import session
from src.config.enviornments import EnviornmentVariables

env = EnviornmentVariables()

class cassandra:
    def __init__(self):
        self.table_name=env.cassandra_input.table_name
        self.keyspace = env.cassandra_input.keyspace

    def table_creation(self):
        # defining table properties
        query = f"""create table IF NOT EXISTS {self.keyspace}.{self.table_name} (
        emp_id int,
        emp_name text,
        city text,
        state text,
        primary key(emp_id)
        );
        """
        try:
          # creating table in cassandra
          session.execute(query)
          print("Table created")
        except Exception as err:
            print(err)
    def insert_records(self, file_name):
        with open(file_name,'r') as f:
          query=f.readline()
          while (query):
              query=query.format(self.keyspace,self.table_name)
              print(query)
              try:
                  session.execute(query)
                  print("Inserted")
                  query=f.readline()
              except Exception as err:
                  print(err)
        
        
        
      
