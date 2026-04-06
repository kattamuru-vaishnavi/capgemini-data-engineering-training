# Phase 4A – Bucketing & Segmentation in PySpark

##  Objective
Understand how continuous data is converted into categories (bucketing/segmentation) and implement different techniques in PySpark.

---


##  Reading Assignments
- Window Functions  
- Filtering Data  
- Grouping Data  

---

##  Core Concept

Bucketing (or segmentation) means dividing continuous values into categories such as:

-  Gold  
-  Silver  
-  Bronze  

This helps in:
- Simplifying analysis  
- Making better business decisions  
- Identifying high-value customers  

---

##  Dataset Used

- **Customers Table** → Customer details  
- **Sales Table** → Transaction data  

---

##  Data Processing Steps

### 1. Calculate Total Spend per Customer

```python
from pyspark.sql.functions import sum

df_total = df2.groupBy("customer_id") \
    .agg(sum("total_amount").alias("total_spend"))
````

---

### 2. Join Customer and Sales Data

```python
df = df1.join(df_total, on="customer_id", how="inner")
```

---

##  Methods to Perform Bucketing

---

### 1️. Conditional Logic (Most Common)

```python
from pyspark.sql.functions import when

df = df.withColumn(
    "segment",
    when(df.total_spend > 100, "Gold")
    .when((df.total_spend >= 50) & (df.total_spend <= 100), "Silver")
    .otherwise("Bronze")
)
```

---

### 2️. SQL CASE Statement

```sql
CASE 
    WHEN total_spend > 100 THEN 'Gold'
    WHEN total_spend BETWEEN 50 AND 100 THEN 'Silver'
    ELSE 'Bronze'
END
```

---

### 3️. Bucketizer 

```python
from pyspark.ml.feature import Bucketizer

splits = [-float("inf"), 50, 100, float("inf")]

bucketizer = Bucketizer(
    splits=splits,
    inputCol="total_spend",
    outputCol="bucket"
)

df = bucketizer.transform(df)
```

---

### 4️. Quantile-based Segmentation

```python
quantiles = df.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

from pyspark.sql.functions import when

df = df.withColumn(
    "quantile_segment",
    when(df.total_spend <= q1, "Low")
    .when((df.total_spend > q1) & (df.total_spend <= q2), "Medium")
    .otherwise("High")
)
```

---

### 5️. Window-based Ranking

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

window = Window.orderBy("total_spend")

df = df.withColumn(
    "rank_pct",
    percent_rank().over(window)
)
```

---

##  Analysis

### Count of Customers per Segment

```python
df.groupBy("segment").count().show()
```

---

### Compare All Methods

```python
df.select(
    "customer_id",
    "total_spend",
    "segment",
    "quantile_segment",
    "rank_pct"
).show()
```

---


##  Reflection Answers

###  Why convert continuous values into categories?

* Simplifies analysis
* Improves decision-making
* Helps in customer targeting

---

###  Business Segmentation vs Technical Bucketing

| Business Segmentation | Technical Bucketing  |
| --------------------- | -------------------- |
| Fixed rules           | Data-driven          |
| Easy to understand    | More accurate        |
| Used in business      | Used in ML/analytics |

---

###  When do fixed thresholds fail?

* When data distribution changes
* When most customers fall into one category
* When thresholds are outdated

---

###  Quantile vs Fixed Rules

* Quantile → Equal distribution
* Fixed Rules → Business logic

---

###  Best Method in Real-world?

 Use a combination:

* Quantile → Understand data
* Conditional logic → Apply business rules

---

##  Conclusion

Bucketing and segmentation are essential techniques in data engineering and analytics.
They help transform raw numerical data into meaningful categories for better insights and decision-making.

