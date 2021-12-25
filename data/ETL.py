import sys
 
# total arguments
n = len(sys.argv)
# print("Total arguments passed:", n-1)
 
# Arguments passed
# print("\nName of Python script:", sys.argv[0])
 
# print("\nArguments passed:", end = " ")
# for i in range(1, n):
# #     print(sys.argv[i], end = " ")
    
# print('\n') 



for i in range(1, n):
#     print(sys.argv[i],end = '\n')
#     print(sys.argv[i].split('=')[0],end = '\n')
#     print(sys.argv[i].split('=')[-1],end = '\n')
    if 'source' == sys.argv[i].split('=')[0]:
        print('source : '+sys.argv[i].split('=')[-1])
        source=sys.argv[i].split('=')[-1]
    elif 'database' == sys.argv[i].split('=')[0]:
        print('database : '+sys.argv[i].split('=')[-1])
        database=sys.argv[i].split('=')[-1]
    elif 'table' == sys.argv[i].split('=')[0]:
        print('table : '+sys.argv[i].split('=')[-1])
        table=sys.argv[i].split('=')[-1]
   
    
print("""\

▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
██░▄▄▄░██░▄▄▄██░▄▄▀█▄▄░▄▄█▄░▄██░▄▄▄░████░▄▄▀█░▄▄▀█▄▄░▄▄█░▄▄▀████░▄▄▄██░▀██░██░▄▄░█▄░▄██░▀██░██░▄▄▄██░▄▄▄██░▄▄▀
██▄▄▄▀▀██░▄▄▄██░▀▀▄███░████░███▄▄▄▀▀████░██░█░▀▀░███░███░▀▀░████░▄▄▄██░█░█░██░█▀▀██░███░█░█░██░▄▄▄██░▄▄▄██░▀▀▄
██░▀▀▀░██░▀▀▀██░██░███░███▀░▀██░▀▀▀░████░▀▀░█░██░███░███░██░████░▀▀▀██░██▄░██░▀▀▄█▀░▀██░██▄░██░▀▀▀██░▀▀▀██░██░
▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

                    """)

import findspark
findspark.init()
from pyspark.sql import SparkSession
from tqdm import tqdm
spark = SparkSession.builder.getOrCreate()

df7 = spark.read.csv('/home/jovyan/data/transaction.csv', sep='|', header=True, inferSchema=True)

df7.createOrReplaceTempView('table1')

df5=spark.sql("""
with table03 as(
with table02 as(
with table01 as (
select custId
        ,cast(transactionDate as date) DateCast
        ,row_number() over(partition by custId order by custId
        ,transactionDate) rn
from table1 
group by custId,transactionDate
order by custId,transactionDate)
select *
        ,date_add(DateCast,-rn) as grp
from table01 )
select custId,grp,min(DateCast),max(DateCast),datediff(max(DateCast),min(DateCast))+1 as consecutive
from table02
group by custId,grp)
select custId,max(consecutive) consecutive_
from table03
group by custId


""")
df5.createOrReplaceTempView('table5')

df2=spark.sql("""


select custId,productSold,sum(unitsSold) sum_
from table1 t1
group by custId,productSold
order by custId,sum_ desc


""")
df2.createOrReplaceTempView('table2')

df3=spark.sql("""

with table01 as(
select custId,productSold,sum(unitsSold) sum_
from table1 t1
group by custId,productSold
order by custId,sum_ desc)
select custId,max(sum_) as fav
from table01
group by custId



""")
df3.createOrReplaceTempView('table3')

df4=spark.sql("""

select t2.custId,t2.productSold
from table2 t2
join table3 t3
on t2.custId = t3.custId and t2.sum_ = t3.fav

""")
df4.createOrReplaceTempView('table4')

df=spark.sql("""

select t4.custId as customer_id,t4.productSold as favourite_product,t5.consecutive_ as longest_streak
from table4 t4
join table5 t5
on t4.custId = t5.custId

""")

from sqlalchemy import create_engine
from datetime import datetime

conn_str = "postgresql+psycopg2://root:password@postgres_db:5432/{0}".format(database)

engine = create_engine(conn_str)
connection = engine.connect()
dict_ConvertType={'string':'varchar'}

stm = ''
for i_,i in enumerate(df.schema.jsonValue()['fields']):
    if i['type'] in dict_ConvertType.keys():
        i['type'] = dict_ConvertType[i['type']]
    if i_ == 0:
        #print(i['name']+' '+i['type'])
        stm += i['name']+' '+i['type']
    else:
        #print(','+i['name']+' '+i['type'])
        stm += ', '+i['name']+' '+i['type']
table_name = table
create_table_stm = 'CREATE TABLE IF NOT EXISTS {0} ({1});'.format(table_name,stm)
res = connection.execute(create_table_stm)
res = connection.execute("TRUNCATE TABLE  {}".format(table_name))
for i in tqdm (range(0,len(df.collect())), desc="Loading..."):
    #print(df.collect()[i][:])
    value_stm = 'INSERT INTO {} VALUES {}'.format(table_name,df.collect()[i][:])
    #print(value_stm)
    res = connection.execute(value_stm)

print('End Process')
sys.exit(0)
