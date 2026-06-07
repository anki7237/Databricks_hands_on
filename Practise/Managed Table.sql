-- Databricks notebook source
CREATE TABLE sales_db.students (
  id INT,
  name STRING,
  marks INT
)
USING DELTA;

-- COMMAND ----------

INSERT INTO sales_db.students VALUES
(1, 'Anki', 95),
(2, 'Rahul', 88),
(3, 'Sneha', 91),
(4, 'Arjun', 76),
(5, 'Priya', 84),
(6, 'Kiran', 67),
(7, 'Meena', 92),
(8, 'Rohit', 73),
(9, 'Divya', 89),
(10, 'Vikram', 81),
(11, 'Asha', 78),
(12, 'Naveen', 85),
(13, 'Pooja', 93),
(14, 'Suresh', 69),
(15, 'Kavya', 87),
(16, 'Tarun', 74),
(17, 'Neha', 96),
(18, 'Ajay', 71),
(19, 'Bhavna', 82),
(20, 'Manoj', 90);

-- COMMAND ----------

DELETE FROM sales_db.students WHERE id=6

-- COMMAND ----------

Update sales_db.students set marks=100
where id=8

-- COMMAND ----------

Select * from sales_db.students

-- COMMAND ----------

VACUUM  sales_db.students

-- COMMAND ----------

SELECT * 
FROM sales_db.students VERSION AS OF 3;

-- COMMAND ----------

RESTORE sales_db.students TO VERSION AS OF 1;

-- COMMAND ----------

Select * from sales_db.students

-- COMMAND ----------

OPTIMIZE sales_db.students

-- COMMAND ----------

Select * from sales_db.students

-- COMMAND ----------

OPTIMIZE sales_db.students ZORDER BY (id)

-- COMMAND ----------

Select * from sales_db.students

-- COMMAND ----------

VACUUM sales_db.students RETAIN 1 HOURS;


-- COMMAND ----------

RESTORE sales_db.students TO VERSION AS OF 1;


-- COMMAND ----------

SELECT * 
FROM sales_db.students VERSION AS OF 3;
