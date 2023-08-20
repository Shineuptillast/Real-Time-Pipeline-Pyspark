from src.cassandra_input.cassandra_load import cassandra

cql = cassandra()
cql.table_creation()

cql.insert_records('data_files_for_cassandra/emp2.txt')
