import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

load_dotenv()

PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DATABASE = os.getenv("POSTGRES_DB")

spark = SparkSession.builder \
    .appName("Pagila_Practice") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

def read_table(table_name:str):
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://postgres:5432/{PG_DATABASE}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .load()

film_df = read_table("film")
category_df = read_table("category")
film_category_df = read_table("film_category")
actor_df = read_table("actor")
film_actor_df = read_table("film_actor")
inventory_df = read_table("inventory")
rental_df = read_table("rental")
payment_df = read_table("payment")
customer_df = read_table("customer")
address_df = read_table("address")
city_df = read_table("city")

# =====================================================================
# TASK 1
# Output the number of movies in each category, sorted in descending order.
# (Вывести количество фильмов в каждой категории, отсортированное по убыванию)
# =====================================================================
print("--- TASK 1 ---")

category_df\
    .join(film_category_df, "category_id")\
    .groupBy("name")\
    .agg(F.count("film_id").alias("count"))\
    .orderBy(F.desc("count")).show()

# =====================================================================
# TASK 2
# Output the 10 actors whose movies rented the most, sorted in descending order.
# (Вывести 10 актеров, чьи фильмы арендовали чаще всего, по убыванию)
# =====================================================================
print("\n--- TASK 2 ---")

actor_df\
    .join(film_actor_df, "actor_id")\
    .join(film_df, "film_id")\
    .join(inventory_df, "film_id")\
    .join(rental_df, "inventory_id")\
    .groupBy("first_name", "last_name")\
    .agg(F.count("rental_id").alias("count"))\
    .orderBy(F.desc("count")).limit(10).show()

# =====================================================================
# TASK 3
# Output the category of movies on which the most money was spent.
# (Вывести категорию фильмов, на которую было потрачено больше всего денег)
# =====================================================================
print("\n--- TASK 3 ---")

category_df\
    .join(film_category_df, "category_id")\
    .join(film_df, "film_id")\
    .join(inventory_df, "film_id")\
    .join(rental_df, "inventory_id")\
    .join(payment_df, "rental_id")\
    .groupBy("name")\
    .agg(F.sum("amount").alias("sum"))\
    .orderBy(F.desc("sum")).limit(1).show()

# =====================================================================
# TASK 4
# Output the names of movies that are not in the inventory.
# (Вывести названия фильмов, которых нет в инвентаре / в наличии)
# =====================================================================
print("\n--- TASK 4 ---")

film_df\
    .join(inventory_df, "film_id","left")\
    .filter("inventory_id is null")\
    .select("title")\
    .show()

# =====================================================================
# TASK 5
# Output the top 3 actors who have appeared most in movies in the “Children” category. 
# If several actors have the same number of movies, output all of them.
# (Вывести топ-3 актеров, которые чаще всего снимались в фильмах категории “Children”. 
# Если у нескольких одинаковое кол-во фильмов, вывести их всех)
# =====================================================================
print("\n--- TASK 5 ---")

actor_df\
    .join(film_actor_df, "actor_id")\
    .join(film_category_df, "film_id")\
    .join(category_df, "category_id")\
    .filter("name = 'Children'")\
    .groupBy("first_name", "last_name")\
    .count()\
    .orderBy(F.desc("count")).limit(3).show()

# =====================================================================
# TASK 6
# Output cities with the number of active and inactive customers (active - customer.active = 1). 
# Sort by the number of inactive customers in descending order.
# (Вывести города с подсчитанным кол-вом активных и неактивных клиентов. 
# Сортировка по кол-ву неактивных клиентов по убыванию)
# =====================================================================
print("\n--- TASK 6 ---")

city_df\
    .join(address_df, "city_id")\
    .join(customer_df, "address_id")\
    .groupBy("city")\
    .agg(
        F.sum(F.when(F.col("active") == 1, 1).otherwise(0)).alias("active"),
        F.sum(F.when(F.col("active") == 0, 1).otherwise(0)).alias("inactive")
    )\
    .orderBy(F.desc("inactive")).show()
         

# # TASK 7
# # Output the category of movies that have the highest number of total rental hours 
# # in the cities (customer.address_id in this city), and that start with the letter “a”. 
# # Do the same for cities with a “-” symbol.
# # (Вывести категорию с наибольшим кол-вом часов аренды в городах на букву “a”. 
# # Сделать то же самое для городов с символом “-”)
# # =====================================================================
# print("\n--- TASK 7 ---")
# # Твой код здесь:

# category_df\
#     .join(film_category_df, "category_id")\
#     .join(film_df, "film_id")\
#     .join(inventory_df, "film_id")\
#     .join(rental_df, "inventory_id")\
#     .join(customer_df, "customer_id")\
#     .join(address_df, "address_id")\
#     .join(city_df, "city_id")\
#     .filter("city LIKE 'a%'").groupBy("name").sum("rental_duration").orderBy(F.desc("sum(rental_duration)")).limit(1).union(
#         category_df\
#             .join(film_category_df, "category_id")\
#             .join(film_df, "film_id")\
#             .join(inventory_df, "film_id")\
#             .join(rental_df, "inventory_id")\
#             .join(customer_df, "customer_id")\
#             .join(address_df, "address_id")\
#             .join(city_df, "city_id")\
#             .filter("city LIKE '%-%'").groupBy("name").sum("rental_duration").orderBy(F.desc("sum(rental_duration)")).limit(1)
#     ).show()

# # =====================================================================
spark.stop()
