import sys
from lib import DataManipulation,DataReader,Utils,logger
from lib.logger import Log4j
from pyspark.sql.functions import *

if __name__ == '__main__':
    if len(sys.argv)<2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating Spark Session")

    spark=Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)
    logger.warn("Created Spark Session")
    
    orders_df=DataReader.read_orders(spark,job_run_env)

    orders_filtered=DataManipulation.filter_closed_orders(orders_df)

    customers_df=DataReader.read_customers(spark,job_run_env)

    joined_df=DataManipulation.join_orders_customers(orders_filtered,customers_df)

    aggregated_result=DataManipulation.count_orders_state(joined_df)

    aggregated_result.show()

    logger.info("End of Main")