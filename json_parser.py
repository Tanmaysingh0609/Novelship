#from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col, lit
from pyspark.sql.types import StructType, ArrayType

spark = SparkSession.builder.master("local[*]").appName("json-parser").getOrCreate()

df = spark.read.format("json").option("multiline", True).option("path", "C:/Users/hp/Downloads/json.txt").load()

df.show()
df.printSchema()


columns = ['_id', 'about', 'address', 'age', 'balance', 'company', 'email', 'eyeColor', 'favoriteFruit', 'friends_id', 'friends_name', 'gender', 'greeting', 'guid', 'index', 'isActive', 'latitude', 'longitude', 'name', 'phone', 'picture', 'registered', 'tags']

def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        print("Outside isinstance loop: " + column_name)
        # Checking column type is ArrayType
        if isinstance(df.schema[column_name].dataType, ArrayType):
            print("Inside isinstance loop of ArrayType: " + column_name)
            df = df.withColumn(column_name, explode_outer(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            print("Inside isinstance loop of StructType: " + column_name)
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    # Selecting columns using column_list from dataframe: df
    df = df.select(column_list)
    return df


read_nested_json_flag = True
while read_nested_json_flag:
    print("Reading Nested JSON File ... ")
    df = read_nested_json(df)
    read_nested_json_flag = False
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            read_nested_json_flag = True
        elif isinstance(df.schema[column_name].dataType, StructType):
            read_nested_json_flag = True

df_cols = df.columns

for ptr in df_cols:
    if ptr not in columns:
        print(ptr)
        df = df.drop(ptr)

for str in columns:
    if str not in df_cols:
        df = df.withColumn(str,lit(None))

final_df = df.select(*[columns])

df.show()