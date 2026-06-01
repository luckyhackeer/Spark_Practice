import pyspark.sql.functions as F
from pyspark.sql.window import Window
from src.database import film_df, category_df, film_category_df, actor_df, film_actor_df, inventory_df, rental_df, payment_df, customer_df, address_df, city_df
def task_1():
    """
    TASK 1
    Output the number of movies in each category, sorted in descending order.
    (Вывести количество фильмов в каждой категории, отсортированное по убыванию)
    """
    print("--- TASK 1 ---")

    category_df\
        .join(film_category_df, "category_id")\
        .groupBy("name")\
        .agg(F.count("film_id").alias("count"))\
        .orderBy(F.desc("count")).show()

def task_2():
    """
    TASK 2
    Output the 10 actors whose movies rented the most, sorted in descending order.
    (Вывести 10 актеров, чьи фильмы арендовали чаще всего, по убыванию)
    """
    
    print("\n--- TASK 2 ---")

    actor_df\
        .join(film_actor_df, "actor_id")\
        .join(inventory_df, "film_id")\
        .join(rental_df, "inventory_id")\
        .groupBy("actor_id","first_name", "last_name")\
        .agg(F.count("rental_id").alias("count"))\
        .orderBy(F.desc("count")).limit(10).show()

def task_3():
    """
    TASK 3
    Output the category of movies on which the most money was spent.
    (Вывести категорию фильмов, на которую было потрачено больше всего денег)
    """

    print("\n--- TASK 3 ---")

    category_df\
        .join(film_category_df, "category_id")\
        .join(inventory_df, "film_id")\
        .join(rental_df, "inventory_id")\
        .join(payment_df, "rental_id")\
        .groupBy("name")\
        .agg(F.sum("amount").alias("sum"))\
        .orderBy(F.desc("sum")).limit(1).show()

def task_4():
    """
    TASK 4
    Output the names of movies that are not in the inventory.
    (Вывести названия фильмов, которых нет в инвентаре / в наличии)
    """

    print("\n--- TASK 4 ---")

    film_df\
        .join(inventory_df, "film_id","left_anti")\
        .select("title")\
        .show()

def task_5():
    """
    TASK 5
    Output the top 3 actors who have appeared most in movies in the “Children” category. 
    If several actors have the same number of movies, output all of them.
    (Вывести топ-3 актеров, которые чаще всего снимались в фильмах категории “Children”. 
    Если у нескольких одинаковое кол-во фильмов, вывести их всех)
    """

    print("\n--- TASK 5 ---")

    actor_df\
        .join(film_actor_df, "actor_id")\
        .join(film_category_df, "film_id")\
        .join(category_df, "category_id")\
        .filter("name = 'Children'")\
        .groupBy("actor_id","first_name", "last_name")\
        .agg(F.count("film_id").alias("count"))\
        .withColumn("rank", F.dense_rank().over(Window.orderBy(F.desc("count"))))\
        .filter("rank <= 3")\
        .orderBy("rank").show()

def task_6():
    """TASK 6
    Output cities with the number of active and inactive customers (active - customer.active = 1). 
    Sort by the number of inactive customers in descending order.
    (Вывести города с подсчитанным кол-вом активных и неактивных клиентов. 
    Сортировка по кол-ву неактивных клиентов по убыванию)
    """

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

def task_7():
    """
    TASK 7
    Output the category of movies that have the highest number of total rental hours 
    in the cities (customer.address_id in this city), and that start with the letter “a”. 
    Do the same for cities with a “-” symbol.
    (Вывести категорию с наибольшим кол-вом часов аренды в городах на букву “a”. 
    Сделать то же самое для городов с символом “-”)
    """
    
    print("\n--- TASK 7 ---")

    rental_with_hours_df = rental_df.withColumn(
        "actual_hours", 
        (F.unix_timestamp(F.coalesce(F.col("return_date"), F.current_timestamp())) - F.unix_timestamp("rental_date")) / 3600
    )

    base_df = category_df.alias("c") \
        .join(film_category_df.alias("fc"), "category_id") \
        .join(inventory_df.alias("i"), "film_id") \
        .join(rental_with_hours_df.alias("r"), "inventory_id") \
        .join(customer_df.alias("cu"), "customer_id") \
        .join(address_df.alias("a"), "address_id") \
        .join(city_df.alias("ci"), "city_id")

    base_df.cache()

    df_city_a = base_df \
        .filter(F.lower(F.col("ci.city")).startswith("a")) \
        .groupBy(F.col("c.name")) \
        .agg(F.round(F.sum("r.actual_hours"), 2).alias("total_hours")) \
        .orderBy(F.desc("total_hours")) \
        .limit(1) \
        .withColumn("group", F.lit("cities_a"))

    df_city_dash = base_df \
        .filter(F.col("ci.city").contains("-")) \
        .groupBy(F.col("c.name")) \
        .agg(F.round(F.sum("r.actual_hours"), 2).alias("total_hours")) \
        .orderBy(F.desc("total_hours")) \
        .limit(1) \
        .withColumn("group", F.lit("cities_dash"))

    df_city_a.union(df_city_dash).show()
