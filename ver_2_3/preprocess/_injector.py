from pyspark.sql import DataFrame
import IO_control.export_file as ex
from UseCases._interface import upload_to_file

    

class original_UL(upload_to_file):
    def __init__(self, OUTPUT_PATH:str, date:str) -> None:
        super().__init__()
        self.OUTPUT_PATH = OUTPUT_PATH
        self.date        = date
    
    def write_parquet_date_file(self, path:str, ps_df:DataFrame) -> None:
        output_path = self.OUTPUT_PATH + path
        ex.write_parquet_date_file(output_path, self.date, ps_df)