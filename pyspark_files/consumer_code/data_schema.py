from pyspark.sql.types import *



class Schema_Kafka:
    
    def __init__(self):
        self.employee_=None
        
    def schema_employee(self):
        
        self.employee_ = StructType([StructField('emp_id',IntegerType()), StructField('emp_name',StringType()), StructField('city',StringType()),StructField('state',StringType())])
        
        return self.employee_
    

sh = Schema_Kafka()

print(sh.schema_employee())