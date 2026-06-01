from src.database import spark
from src.tasks import task_1, task_2, task_3, task_4, task_5, task_6, task_7

if __name__ == "__main__":
    task_1()
    task_2()
    task_3()
    task_4()
    task_5()
    task_6()
    task_7()
    spark.stop()
