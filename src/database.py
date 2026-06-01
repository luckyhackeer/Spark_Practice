from pyspark.sql import SparkSession
from src.config import settings

PG_USER = settings.POSTGRES_USER
PG_PASSWORD = settings.POSTGRES_PASSWORD
PG_DATABASE = settings.POSTGRES_DB
PG_HOST = settings.POSTGRES_HOST
PG_PORT = settings.POSTGRES_PORT

spark = SparkSession.builder \
    .appName("Pagila_Practice") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

def read_table(table_name:str):
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}") \
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