
import logging

from launcher.config.config import Config
from pyspark.sql import SparkSession

class LocalDataProvider:
    """Provide services for loading and saving dataframes localy."""

    def __init__(self, conf: Config):
        """Init."""
        self.workflows_path = conf.get("adf_workflows_path")
        self.snowflake_path = conf.get("adf_snowflake_path")
        self.run_id = conf.get("job_run_id")
        self.conf = conf
        self.run_date = conf.get("job_run_date")
        self.spark = SparkSession.builder.master("local[1]").appName("ADF-local").getOrCreate()
        self.logger = logging.getLogger("adf.utilities")
        self.logger.info("Created LocalDataProvider instance")
