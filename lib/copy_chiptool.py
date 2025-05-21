from lib.ChipException import ChipException
from lib.chiputil import chiputilbox
from lib.rrntool import RrnToolBox
class ChipTool:
    @staticmethod
    def L3DCharCheck(c):
        if c in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']:
            return False
        return True
    @staticmethod
    def generate_chip(array_lot_id, tft_chip_id):
        auo_chip_id = None
        if array_lot_id is None or array_lot_id.strip() == "":
            raise ChipException("The Array_lot_id is null.")
        elif len(array_lot_id.strip()) < 6:
            raise ChipException("The Array_lot_id is not the effective form.")
        elif tft_chip_id is None or tft_chip_id.strip() == "":
            raise ChipException("The Tft_chip_id is null.")

        for i in range(len(tft_chip_id.strip())):
            num = chiputilbox.change_char(tft_chip_id.strip()[i])
            if num == 99:
                raise ChipException("Tft_chip_id has not the effective character.")
        try:
            if len(tft_chip_id.strip()) == 7:
                if array_lot_id[4] == 'H':
                    auo_chip_id = array_lot_id.strip()[:2] + array_lot_id.strip()[3:4] + tft_chip_id.strip() + "ZZ"
                else:
                    auo_chip_id = array_lot_id.strip()[:3] + tft_chip_id.strip() + "ZZ"
            elif len(tft_chip_id.strip()) == 9:
                auo_chip_id = array_lot_id.strip()[:3] + tft_chip_id.strip()
            elif len(tft_chip_id.strip()) == 10:
                auo_chip_id = tft_chip_id.strip() + "ZY"
            elif len(tft_chip_id.strip()) == 11 and tft_chip_id[0] == 'S':
                auo_chip_id = tft_chip_id.strip() + "X"
            elif len(tft_chip_id.strip()) == 11 and tft_chip_id[0] != 'S':
                auo_chip_id = tft_chip_id.strip() + "V"
            elif len(tft_chip_id.strip()) == 12:
                auo_chip_id = tft_chip_id.strip()
                # QDI tft_chip_id
            if len(tft_chip_id.strip()) == 15:
                n5 = tft_chip_id[4]
                n6 = tft_chip_id[5]
                if n5 in ['H', 'G', 'A', 'N', '5']:
                    auo_chip_id = chiputilbox.transL6B(tft_chip_id)
                elif n6 in ['B', 'G', 'C']:
                    auo_chip_id = chiputilbox.transL5D(tft_chip_id)
                elif n5 != 'G' and ChipTool.L3DCharCheck(n6):
                    auo_chip_id = chiputilbox.transL3D(tft_chip_id)
                else:
                    raise ChipException("Tft_chip_id is not the effective form.")
            # 16-len tft_chip_id
            elif len(tft_chip_id.strip()) == 16:
                if tft_chip_id[5] == 'B':
                    auo_chip_id = chiputilbox.transL5D16(tft_chip_id)
                elif tft_chip_id[5] != 'B' and tft_chip_id[11] == '-' and tft_chip_id[0] != 'H':
                    auo_chip_id = chiputilbox.transL3C16(tft_chip_id)
                elif tft_chip_id[5] != 'B' and tft_chip_id[0] == 'H':
                    auo_chip_id = chiputilbox.transL3D16(tft_chip_id)
            # 17-len tft_chip_id
            elif len(tft_chip_id.strip()) == 17:
                if tft_chip_id[5] == 'B':
                    auo_chip_id = chiputilbox.transL5D17(tft_chip_id)
                elif tft_chip_id[5] != 'B':
                    auo_chip_id = chiputilbox.transL3D17(tft_chip_id)
                else:
                    raise ChipException("Tft_chip_id is not the effective form.")
        except Exception as e:
                raise ChipException(str(e))
        return auo_chip_id