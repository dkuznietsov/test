from lib.rrntool import RrnToolBox
from lib.chiptool import ChipTool
from lib.RrnException import RrnException
from lib.ChipException import ChipException
def CalProcessData(array_lot_id,tft_chip_id):
    try:
        tft_chip_id_rrn = RrnToolBox.generate_rrn(array_lot_id, tft_chip_id)
        new_chip = ChipTool.generate_chip(array_lot_id, tft_chip_id)
        print(f"ARRAY_LOT_ID: {array_lot_id}")
        print(f"TFT_CHIP_ID : {tft_chip_id}")
        print(f"TFT_CHIP_ID_RRN: {tft_chip_id_rrn}")
        print(f"NEW_CHIP: {new_chip}")
        return tft_chip_id_rrn, new_chip
    except RrnException as e:
        # 處理RrnException錯誤
        print(f"RrnException: {str(e)}")
        return None, None
    except ChipException as e:
        # 處理ChipException錯誤
        print(f"ChipException: {str(e)}")
        return None, None
    except Exception as e:
        # 處理其他未預期的錯誤
        print(f"Unexpected error: {str(e)}")
        return None, None
dataset1=[{'array_lot_id': 'ZD12HBMQ40', 'tft_chip_id': 'ZD12HBMQ40T1601'}]
#dataset2=[]
for data in dataset1:
    array_lot_id = data['array_lot_id']
    tft_chip_id = data['tft_chip_id']
    tft_chip_id_rrn, new_chip = CalProcessData(array_lot_id, tft_chip_id)