import os
import boto3
import json
import urllib.parse
import binascii
import gzip
import logging

from elasticsearch import ElasticsearchException
from botocore.exceptions import ClientError
from common.elasticsearch_client import *
from common.constants import *
from common.logger_utility import *
from common.constants import *
from bucket_handler_lambda.bucket_event_lambda_handler import *
from odevalidator import *
from queue import Queue

### Debugging settings
VERBOSE_OUTPUT = True if os.environ.get('VERBOSE_OUTPUT') == 'TRUE' else False

### Data source settings
USE_STATIC_PREFIXES = True if os.environ.get('USE_STATIC_PREFIXES') == 'TRUE' else False
STATIC_PREFIXES = os.environ.get('STATIC_PREFIXES').split(',')

class CVPBucketEventHandler(HandleBucketEvent):

    def _is_gz_file(self, filepath):
        with open(filepath, 'rb') as test_f:
            return binascii.hexlify(test_f.read(2)) == b'1f8b'
            
    def _getRecordsQueue(self, bucket_name, key):
        localFileName = '/tmp/localFile'
        LoggerUtility.logInfo("============================================================================")
        LoggerUtility.logInfo("Analyzing file '%s'" % bucket_name + key)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        bucket.download_file(key, localFileName)

        dataQueue = Queue()
        if self._is_gz_file(localFileName):
            LoggerUtility.logDebug("file is gz compressed")
            with gzip.open(localFileName) as dataFile:
                for line in dataFile:
                    if line.strip():
                        #LoggerUtility.logDebug(line.decode('utf-8'))
                        dataQueue.put(line.decode('utf-8'))
        else:
            with open(localFileName) as dataFile:
                LoggerUtility.logDebug("file is not compressed")
                for line in dataFile:
                    if line.strip():
                        #LoggerUtility.logDebug(line)
                        dataQueue.put(line)

        if dataQueue.qsize() == 0:
            logger.warning("Could not find any records to be validated in S3 file '%s'." % filename)
            logger.warning("============================================================================")

        LoggerUtility.logInfo('Line count: ' + str(dataQueue.qsize()))
        
        os.remove(localFileName)
        
        return dataQueue

    def _createMetadataObject(self, s3_head_object, key, bucket_name, path):
        metadata = {
            Constants.KEY_REFERENCE: key,
            Constants.CONTENT_LENGTH_REFERENCE: s3_head_object[Constants.CONTENT_LENGTH_REFERENCE],
            Constants.SIZE_MIB_REFERENCE: s3_head_object[Constants.CONTENT_LENGTH_REFERENCE] / 1024 ** 2,
            Constants.LAST_MODIFIED_REFERENCE: s3_head_object[Constants.LAST_MODIFIED_REFERENCE].isoformat(),
            Constants.CONTENT_TYPE_REFERENCE: s3_head_object[Constants.CONTENT_TYPE_REFERENCE],
            Constants.ETAG_REFERENCE: s3_head_object[Constants.ETAG_REFERENCE],
            Constants.DATASET_REFERENCE: key.split('/')[0],
            Constants.ENVIRONMENT_NAME: os.environ["ENVIRONMENT_NAME"]
        }
        
        keys = path.split('/')
        length = len(keys)
        
        if length != 0:
            data_provider_type_value = keys[length - 2]
            data_provider_type_metadata = {
                Constants.DATA_PROVIDER_REFERENCE: data_provider_type_value
            }
            metadata.update(data_provider_type_metadata)

            data_type_value = keys[length - 1]
            data_type_metadata = {
                Constants.DATA_TYPE_REFERENCE: data_type_value
            }
            metadata.update(data_type_metadata)
        else:
            data_provider_type_value = key
            data_provider_type_metadata = {
                Constants.DATA_PROVIDER_REFERENCE: data_provider_type_value
            }
            metadata.update(data_provider_type_metadata)
            
        message_count_value = self.recordQueue.qsize()
        message_count_metadata = {
            Constants.MESSAGE_COUNT: message_count_value
        }
        metadata.update(message_count_metadata)

        LoggerUtility.logInfo("METADATA: " + str(metadata))
        LoggerUtility.logInfo("S3 Head Object: " + str(s3_head_object))
        return metadata

    def _createValidationDataObject(self, s3_head_object, key, bucket_name, datatype):

        test_case = TestCase()
        validation_results = test_case.validate_queue(self.recordQueue)
        
        num_error_messages = 0
        num_messages_total = 0
        num_errors = 0
        num_validations = 0
        total_validation_count = 0
        total_validations_failed = 0
        records_analyzed = 0
        error_dict = {}
        for result in validation_results:
            num_messages_total += 1
            num_validations += len(result.field_validations)
            isValid = True
            for validation in result.field_validations:
                if validation.valid == False:
                    isValid = False
                    num_errors += 1
                    validation_message = "Invalid field '" + validation.field_path + "' due to " + validation.details
                    if validation_message in error_dict:
                        error_dict[validation_message] += 1
                    else:
                        error_dict[validation_message] = 1
            if not isValid:
                num_error_messages += 1

        total_validation_count += num_validations
        total_validations_failed += num_errors
        if num_errors > 0:
            LoggerUtility.logWarning("Validation has FAILED for file '%s' of type '%s'. Detected %d errors out of %d total validation checks." % (bucket_name + key, datatype, num_errors, num_validations))
            if VERBOSE_OUTPUT:
                for error in error_dict:
                    LoggerUtility.logWarning("[Error: '%s', Occurrences: '%d'" % (error, error_dict[error]))
            LoggerUtility.logWarning("============================================================================")
        else:
            LoggerUtility.logInfo("Validation has PASSED for file '%s' of type '%s'. Detected no errors out and performed %d total validation checks." % (bucket_name + key, datatype, num_validations))
            LoggerUtility.logInfo("===========================================================================")

        LoggerUtility.logInfo("[CANARY FINISHED] Validation complete, detected %d errors out of %d validations." % (total_validations_failed, total_validation_count))

        return validation_results, num_messages_total - num_error_messages, num_error_messages

    def _publishCustomMetricsToCloudwatch(self, bucket_name, metadata, num_valid_messages, num_error_messages):
        try:
            if bucket_name == os.environ["SUBMISSIONS_BUCKET_NAME"] and metadata["Dataset"] == "cv":
                cloudwatch_client = boto3.client('cloudwatch')
                cloudwatch_client.put_metric_data(
                    Namespace='dot-sdc-cv-submissions-bucket-metric',
                    MetricData=[
                        {
                            'MetricName' : 'Counts by provider and datatype',
                            'Dimensions' : [
                                {
                                    'Name' : 'DataProvider',
                                    'Value': metadata["DataProvider"]
                                },
                                {
                                    'Name' : 'DataType',
                                    'Value': metadata["DataType"]
                                }
                            ],
                            'Value' : 1,
                            'Unit': 'Count'
                        },
                        {
                            'MetricName' : 'Valid counts by provider and datatype',
                            'Dimensions' : [
                                {
                                    'Name' : 'DataProvider',
                                    'Value': metadata["DataProvider"]
                                },
                                {
                                    'Name' : 'DataType',
                                    'Value': metadata["DataType"]
                                }
                            ],
                            'Value' : num_valid_messages,
                            'Unit': 'Count'
                        },
                        {
                            'MetricName' : 'Invalid counts by provider and datatype',
                            'Dimensions' : [
                                {
                                    'Name' : 'DataProvider',
                                    'Value': metadata["DataProvider"]
                                },
                                {
                                    'Name' : 'DataType',
                                    'Value': metadata["DataType"]
                                }
                            ],
                            'Value' : num_error_messages,
                            'Unit': 'Count'
                        },
                        {
                            'MetricName' : 'Data file count by provider and datatype',
                            'Dimensions' : [
                                {
                                    'Name' : 'DataProvider',
                                    'Value': metadata["DataProvider"]
                                },
                                {
                                    'Name' : 'DataType',
                                    'Value': metadata["DataType"]
                                }
                            ],
                            'Value' : 1,
                            'Unit': 'Count'
                        }
                    ]
                )
        except Exception as e:
            LoggerUtility.logError(e)
            LoggerUtility.logError("Failed to publish custom cloudwatch metrics")
            raise e


    def handleBucketEvent(self, event, context):
        if VERBOSE_OUTPUT:
            Constants.LOGGER_LOG_LEVEL_ENV_VAR = "DEBUG"
        else:
            Constants.LOGGER_LOG_LEVEL_ENV_VAR = "INFO"
        LoggerUtility.setLevel()
        bucket_name, object_key = self._fetchS3DetailsFromEvent(event)
        s3_head_object = self._getS3HeadObject(bucket_name, object_key)
        self.recordQueue = self._getRecordsQueue(bucket_name, object_key)
        
        s3res = boto3.resource('s3')
        bucket = s3res.Bucket(bucket_name)
        pathtoconfig = None
        
        for path in STATIC_PREFIXES:
            pathlen = len(path)
            keypath = object_key[:pathlen]
            if(path == keypath):
                pathtoconfig = path
                LoggerUtility.logDebug("File path matched to STATIC_PREFIXES: " + path)
                break
        if pathtoconfig is not None:
            LoggerUtility.logDebug("object_key count " + str(object_key.count('/')))
            LoggerUtility.logDebug("pathtoconfig count " + str(pathtoconfig.count('/') + 1))
            if(object_key.count('/') > pathtoconfig.count('/') + 1):
                ini_key = pathtoconfig + '/' + 'config.ini'
                LoggerUtility.logInfo("Grabbing file: " + ini_key)
                bucket.download_file(ini_key, '/tmp/config')
                metadata = self._createMetadataObject(s3_head_object, object_key, bucket_name, pathtoconfig)
                LoggerUtility.logInfo("Provider " + metadata["DataProvider"])
                LoggerUtility.logInfo("Type " + metadata["DataType"])
                validationData, num_valid_messages, num_error_messages = self._createValidationDataObject(s3_head_object, object_key, bucket_name, metadata["DataType"])
                self._pushMetadataToElasticsearch(bucket_name, metadata)
                self._publishCustomMetricsToCloudwatch(bucket_name, metadata, num_valid_messages, num_error_messages)
            else:
                LoggerUtility.logWarning("File in root directory is ignored")
        else:
            LoggerUtility.logError("File path not found in STATIC_PREFIXES")