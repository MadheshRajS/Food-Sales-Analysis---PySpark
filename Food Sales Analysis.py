# Databricks notebook source
# DBTITLE 1,Required Functions
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# DBTITLE 1,Dataset loaded location
/FileStore/tables/menu_csv.txt
/FileStore/tables/sales_csv.txt

# COMMAND ----------

# DBTITLE 1,Sales DataFrame
# MAGIC %python
# MAGIC from pyspark.sql.types import *
# MAGIC schema=StructType([
# MAGIC     StructField('product_id',IntegerType(),True),
# MAGIC     StructField('customer_id',StringType(),True),
# MAGIC     StructField('order_date',DateType(),True),
# MAGIC     StructField('location',StringType(),True),
# MAGIC     StructField('source_order',StringType(),True)
# MAGIC ])
# MAGIC sales_df=spark.read.format("csv").option('inferschema','true').schema(schema).load('/FileStore/tables/sales_csv.txt')
# MAGIC sales_df.show()

# COMMAND ----------

# DBTITLE 1,Year,Month,Date
# MAGIC %python
# MAGIC sales_df=sales_df.withColumn('order_year',date_format(col('order_date'),'yyyy'))
# MAGIC sales_df=sales_df.withColumn('order_month',date_format(col('order_date'),'MM'))
# MAGIC sales_df=sales_df.withColumn('order_quarter',quarter(sales_df.order_date))
# MAGIC sales_df.show()

# COMMAND ----------

# DBTITLE 1,Menu DataFrame
# MAGIC %python
# MAGIC schema=StructType([
# MAGIC     StructField('product_id',IntegerType(),True),
# MAGIC     StructField('product_name',StringType(),True),
# MAGIC     StructField('price',StringType(),True)
# MAGIC ])
# MAGIC menu_df=spark.read.format("csv").option('inferschema','true').schema(schema).load('/FileStore/tables/menu_csv.txt')
# MAGIC menu_df.show()

# COMMAND ----------

# DBTITLE 1,Joining the two(Sales,Menu) dataframes
total_amount_spent=sales_df.join(menu_df,on='product_id')
total_amount_spent.show()

# COMMAND ----------

# DBTITLE 1,Total Amount spent by each customer
tac=total_amount_spent.groupby('customer_id').agg(sum('price').alias('total_amount_spent')).orderBy(col('customer_id').asc())
display(tac)

# COMMAND ----------

# DBTITLE 1,Total amount spent by each food category
taf=total_amount_spent.groupby('product_name').agg(sum('price').alias('total_amount_spent_food')).orderBy(col('total_amount_spent_food').desc())
display(taf)

# COMMAND ----------

# DBTITLE 1,Total amount of sales in each month
tam=total_amount_spent.groupby('order_month').agg(sum('price').alias('sales in each month')).orderBy(col('sales in each month').desc())
display(tam)

# COMMAND ----------

# DBTITLE 1,Total amount of sales in each year
tay=total_amount_spent.groupby('order_year').agg(sum('price').alias('sales in each year')).orderBy(col('sales in each year').desc())
display(tay)

# COMMAND ----------

# DBTITLE 1,Total amount of sales in each quarter
taq=total_amount_spent.groupby('order_quarter').agg(sum('price').alias('sales in each quarter')).orderBy(col('sales in each quarter').desc())
display(taq)

# COMMAND ----------

# DBTITLE 1,How many times each product has been purchased 
pp=total_amount_spent.groupBy('product_id','product_name').agg(count('product_id').alias('Times_product_purchased')).orderBy(col('Times_product_purchased').desc()).drop('product_id')
display(pp)

# COMMAND ----------

# DBTITLE 1,Top 5 order items
t5=total_amount_spent.groupBy('product_id','product_name').agg(count('product_id').alias('Times_product_purchased')).orderBy(col('Times_product_purchased').desc()).drop('product_id').limit(5)
display(t5)

# COMMAND ----------

# DBTITLE 1,Top Ordered item
toi=total_amount_spent.groupBy('product_id','product_name').agg(count('product_id').alias('Times_product_purchased')).orderBy(col('Times_product_purchased').desc()).drop('product_id').limit(1)
display(toi)

# COMMAND ----------

# DBTITLE 1,Frequency of Customer visited to Restaurant
res=total_amount_spent.filter(col('source_order')=='Restaurant').groupBy('customer_id').agg(countDistinct('order_date').alias('Times visited')).orderBy(col('Times visited').desc())
display(res)

# COMMAND ----------

# DBTITLE 1,Total sales by each country
tc=total_amount_spent.groupBy('location').agg(sum('price').alias('total country sales')).orderBy(col('total country sales').desc())
display(tc)

# COMMAND ----------

# DBTITLE 1,Total sales by each OrderSource
ts=total_amount_spent.groupBy('source_order').agg(sum('price').alias('total source sales')).orderBy(col('total source sales').desc())
display(ts)

# COMMAND ----------


