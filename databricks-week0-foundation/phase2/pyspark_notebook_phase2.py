from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28),
    (4, "Vaishnavi", "Bangalore", 25),
    (5, "Ramya", "Hyderabad", 27),
    (6, "Yasodha", "Chennai", 28)
], ["customer_id", "customer_name", "city", "age"])

orders=spark.createDataFrame([
  (101,1,1550),
  (102,5,5350),
  (104,3,1750),
  (105,5,3050),
  (106,6,7000),
  (107,None,2000)
],["order_id","customer_id","amount"])

customers = customers.dropna(subset=["customer_id"])
orders = orders.dropna(subset=["customer_id"])

#1.Total order amount for each customer
orders.groupBy("customer_id").sum("amount").show()            

#2. Top 3 customers by total spend
orders.groupBy("customer_id").sum("amount").orderBy("sum(amount)", ascending=False).limit(3).show()      
                       
#3. Customers with no orders
customers.join(orders,on="customer_id",how="left_anti").show()  
                       
#4. City-wise total revenue
customers.join(orders,on="customer_id").groupBy("city").sum("amount").show()              

#5. Average order amount per customer    
orders.groupBy("customer_id").avg("amount").show()

#6. Customers with more than one order
orders.groupBy("customer_id").count().filter("count > 1").show()
                       
#7. Sort customers by total spend descending
orders.groupBy("customer_id").sum("amount").orderBy("sum(amount)",ascending=False).show()
