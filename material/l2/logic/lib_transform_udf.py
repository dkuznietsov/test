from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from lib.rrntool import RrnToolBox
from lib.RrnException import RrnException

def get_transform_rrn_udf():
    def generate_rrn(array_lot_id, tft_chip_id):
        try:    
            return RrnToolBox.generate_rrn(array_lot_id, tft_chip_id)
        except RrnException:
            return None
        
    generate_rrn_udf = F.udf(generate_rrn, StringType())
    print("????")

    return generate_rrn_udf
