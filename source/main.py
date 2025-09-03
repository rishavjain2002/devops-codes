import boto3
import time
from helper import run_query


QUERY_INTERVAL_SECONDS = 300 
# 3000 - 50 mins

print("Starting continuous restartCount monitoring...")
run_query()

# while True:
#     run_query()
    # time.sleep(QUERY_INTERVAL_SECONDS)
print("done")