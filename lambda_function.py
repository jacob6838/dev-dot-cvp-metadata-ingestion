from bucket_handler_lambda.cvp_bucket_event_lambda_handler import *
import traceback

log = logging.getLogger()
log.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        LoggerUtility.logInfo(event)
        handle_bucket_event = CVPBucketEventHandler()
        handle_bucket_event.handleBucketEvent(event, context)
    except Exception as e:
        LoggerUtility.logError(e)
        traceback.print_exc()
