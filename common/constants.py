class Constants:

    LOGGER_NAME = "datalake-quickstart-logger"
    LOGGER_LOG_LEVEL_ENV_VAR = "LOG_LEVEL"
    LOGGER_DEFAULT_LOG_LEVEL = "INFO"
    ELASTICSEARCH_SERVICE_CLIENT = "es"
    S3_SERVICE_CLIENT = "s3"
    KIBANA_INDEX_NAME = ".kibana"
    KIBANA_JSON_FILENAME = 'kibana_metadata_visualizations.json'
    DEFAULT_INDEX_ID = "metadata"
    CONFIG_DOCUMENT_TYPE = "config"
    INDEX_PATTERN_DOCUMENT_TYPE = "index-pattern"
    ACCESS_KEY_ENV_VAR = "AWS_ACCESS_KEY_ID"
    SECRET_KEY_ENV_VAR = "AWS_SECRET_ACCESS_KEY"
    REGION_ENV_VAR = "AWS_REGION"
    SESSION_TOKEN_ENV_VAR = "AWS_SESSION_TOKEN"
    ES_ENDPOINT_ENV_VAR = "ELASTICSEARCH_ENDPOINT"
    TITLE_REFERENCE = "title"
    TIME_FIELD_NAME_REFERENCE = "timeFieldName"
    LAST_MODIFIED_REFERENCE = "LastModified"
    DEFAULT_INDEX_REFERENCE = "defaultIndex"
    CONTENT_TYPE_REFERENCE = "ContentType"
    CONTENT_LENGTH_REFERENCE = "ContentLength"
    KEY_REFERENCE = "key"
    SIZE_MIB_REFERENCE = "SizeMiB"
    ETAG_REFERENCE = "ETag"
    DATASET_REFERENCE = "Dataset"
    STATE_REFERENCE = "State"
    TRAFFIC_TYPE_REFERENCE = "TrafficType"
    TABLE_NAME_REFERENCE = "TableName"
    CURATION_VERSION = "20171031"
    ENVIRONMENT_NAME = "Environment"
    DATA_PROVIDER_REFERENCE = "DataProvider"
    DATA_TYPE_REFERENCE = "DataType"
    MESSAGE_COUNT = "MessageCount"

    def __setattr__(self, attr, value):
        if hasattr(self, attr):
            raise Exception("Attempting to alter read-only value")

        self.__dict__[attr] = value
