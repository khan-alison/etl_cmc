from extract.base_extract import BaseExtractor


class LocalExtractor(BaseExtractor):
    """Extractor for local files"""

    def read_file(self, path: str):
        """Reads a file from the local system"""
        return self.spark.read.format(self.file_format)\
            .option("header", True)\
            .option("inferSchema", True)\
            .load(path)
