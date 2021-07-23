class Log4J:
    # This method is called when an object is created from a
    # class and it allows the class to initialize the attributes of the class.
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j  # this is to get the jvm object
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger("guru.anantha.spark.examples" + "." + app_name)
        # use this logger to log the messages
        # guru.anantha.spark.examples  - this is added in the log4j.propeties under application logs.

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)
