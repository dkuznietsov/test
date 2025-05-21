from lib.RrnException import RrnException
from lib.chiputil import chiputilbox
class RrnToolBox:
    @staticmethod
    def char2Bit(_tft_chip_id_rrn):
        result = 0
        for i in range(len(_tft_chip_id_rrn)):
            c = _tft_chip_id_rrn[i]
            num = chiputilbox.change_char(c)
            if num == 99:
                raise RrnException("Tft_chip_id has is not the effective character.")
            else:
                result += num * (36 ** (len(_tft_chip_id_rrn) - i - 1))
        return str(result)
    @staticmethod
    def convertM01ChipToL3d(chip_id_label):
        chip_id_label_l3d = ""
        for i in range(len(chip_id_label)):
            id_temp = chip_id_label[i:i+1]
            chip_id_label_l3d += RrnToolBox.getPosL3d(id_temp)
        return chip_id_label_l3d
    @staticmethod
    def transChipL3D17(tft_chip_id):
        t1 = tft_chip_id[:6]
        t2 = tft_chip_id[15:17]
        x1 = tft_chip_id[6:10]  # 6~10 user 0~Z replace
        x2 = tft_chip_id[11:13]  # 11~13 user 0~Z replace
        x3 = tft_chip_id[13:15]  # 13~15 user 0~Z replace
        i1 = int(x1)
        i2 = int(x2)
        i3 = int(x3)
        m1 = i1 // 34
        n1 = i1 % 34
        m11 = m1 // 34
        n11 = m1 % 34
        n2 = i2 % 34
        n3 = i3 % 34
        new_tft_chip_id = t1 +  chiputilbox.change_int(m11) +  chiputilbox.change_int(n11) + \
             chiputilbox.change_int(n1) +  chiputilbox.change_int(n2) + chiputilbox.change_int(n3) + t2
        return new_tft_chip_id
    @staticmethod
    def transChipL3D16(tft_chip_id):
        t1 = tft_chip_id[:6]
        t2 = tft_chip_id[13:16]
        x1 = tft_chip_id[6:10]  # 7~10 user 0~Z replace
        x2 = tft_chip_id[11:13]  # 12~13 user 0~Z replace
        i1 = int(x1)
        i2 = int(x2)
        m1 = i1 // 34
        n1 = i1 % 34
        m11 = m1 // 34
        n11 = m1 % 34
        n2 = i2 % 34
        new_tft_chip_id = t1 +  chiputilbox.change_int(m11) +  chiputilbox.change_int(n11) + \
             chiputilbox.change_int(n1) +  chiputilbox.change_int(n2)  + t2
        return new_tft_chip_id
    @staticmethod
    def transChipL3D15(tft_chip_id):
        t1 = tft_chip_id[:6]
        t2 = tft_chip_id[13:15]
        x1 = tft_chip_id[6:10]  # 7~10 user 0~Z replace
        x2 = tft_chip_id[11:13]  # 12~13 user 0~Z replace
        i1 = int(x1)
        i2 = int(x2)
        m1 = i1 // 34
        n1 = i1 % 34
        m11 = m1 // 34
        n11 = m1 % 34
        n2 = i2 % 34
        new_tft_chip_id = t1 +  chiputilbox.change_int(m11) +  chiputilbox.change_int(n11) + \
             chiputilbox.change_int(n1) +  chiputilbox.change_int(n2) + t2
        return new_tft_chip_id
    @staticmethod
    def getPosL3d(pos):
        convert_dict = {
            '1': '0', '2': '1', '3': '2', '4': '3',
            '5': '4', '6': '5', '7': '6', '8': '7',
            '9': '8', 'A': '9', 'B': 'A', 'C': 'B',
            'D': 'C', 'E': 'D', 'F': 'E', 'G': 'F',
            'H': 'G', 'J': 'H', 'K': 'J', 'L': 'K',
            'M': 'L', 'N': 'M', 'P': 'N', 'Q': 'P',
            'R': 'Q', 'S': 'R', 'T': 'S', 'U': 'T',
            'V': 'U', 'W': 'V', 'X': 'W', 'Y': 'X',
            'Z': 'Y'
        }
        pos_l3d = convert_dict.get(pos, '')
        return pos_l3d
    @staticmethod
    def generate_rrn(array_lot_id, tft_chip_id):
        if array_lot_id is None or array_lot_id.strip() == "":
            raise RrnException("The Array_lot_id is null.")
        elif len(array_lot_id.strip()) < 6:
            raise RrnException("The Array_lot_id is not the effective form.")
        elif tft_chip_id is None or tft_chip_id.strip() == "":
            raise RrnException("The Tft_chip_id is null.")
        tft_chip_id_rrn = ""
        if len(tft_chip_id.strip()) == 7:
            try:
                tft_chip_id_rrn = RrnToolBox.char2Bit(
                    array_lot_id.strip()[:3] + tft_chip_id.strip())
            except RrnException as e:
                raise e
        elif len(tft_chip_id.strip()) == 15:
            try:
                tft_chip_id_rrn = RrnToolBox.char2Bit(
                    tft_chip_id.strip()[:3]) +  RrnToolBox.char2Bit(tft_chip_id.strip()[3:15])
            except RrnException as e:
                raise e
        elif len(tft_chip_id.strip()) == 17:
            try:
                tft_chip_id =RrnToolBox.transChipL3D17(tft_chip_id)
                tft_chip_id_rrn = RrnToolBox.char2Bit(array_lot_id.strip()[:3] + tft_chip_id.strip())
            except RrnException as e:
                raise e
        elif len(tft_chip_id.strip()) == 16:
            try:
                tft_chip_id = RrnToolBox.transChipL3D16(tft_chip_id)
                tft_chip_id_rrn = RrnToolBox.char2Bit(array_lot_id.strip()[:3] + tft_chip_id.strip())
            except RrnException as e:
                raise e
        elif len(tft_chip_id.strip()) == 13:
            try:
                tft_chip_id_rrn = RrnToolBox.char2Bit(array_lot_id.strip()[:3] + tft_chip_id.strip())
            except RrnException as e:
                raise e
        elif len(tft_chip_id.strip()) == 12:
            try:
                tft_chip_id_rrn = RrnToolBox.char2Bit(tft_chip_id.strip())
            except RrnException as e:
                raise e
        elif len(tft_chip_id.strip()) == 9:
            try:
                tft_chip_id_rrn = RrnToolBox.char2Bit(array_lot_id.strip()[:3] + tft_chip_id.strip())
            except RrnException as e:
                raise e
        elif len(tft_chip_id.strip()) == 10:
            try:
                tft_chip_id_rrn = RrnToolBox.char2Bit(tft_chip_id.strip() + "ZY")
            except RrnException as e:
                raise e
        elif len(tft_chip_id.strip()) == 11 and tft_chip_id[0] != 'S':
            try:
                tft_chip_id_rrn = RrnToolBox.char2Bit(tft_chip_id.strip() + 'V')
            except RrnException as e:
                raise e
        else:
            raise RrnException("Tft_chip_id is not the effective form.")
        return tft_chip_id_rrn