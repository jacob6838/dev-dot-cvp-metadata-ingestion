from lambda_function import *
import logging
import sys

# logger setup - to include console:
# https://stackoverflow.com/questions/14058453/making-python-loggers-output-all-messages-to-stdout-in-addition-to-log/14058475
log = logging.getLogger()
log.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

# run lambda function locally:
# https://stackoverflow.com/questions/43121621/how-can-i-test-lambda-in-local-using-python
if __name__ == "__main__":
    event = [{"event":"local"}]
    context = []
    lambda_handler(event, context)

# deploy package creation:
# "C:\Program Files\7-Zip\7z.exe" a -tzip deploy\lambda_function.zip *.py

# publish driver SNS:
# arn:aws:sns:us-east-1:911061262852:v-dev-topic
