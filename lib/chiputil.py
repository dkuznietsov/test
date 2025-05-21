class chiputilbox:
    @staticmethod
    def change_char(c):
        convert_dict = {'0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, 
                        '6': 6, '7': 7, '8': 8, '9': 9, 'A': 10, 'B': 11, 
                        'C': 12, 'D': 13, 'E': 14, 'F': 15, 'G': 16, 'H': 17,
                        'I': 18, 'J': 19, 'K': 20, 'L': 21, 'M': 22, 'N': 23, 
                        'O': 24, 'P': 25, 'Q': 26, 'R': 27, 'S': 28, 'T': 29, 
                        'U': 30, 'V': 31, 'W': 32, 'X': 33, 'Y': 34, 'Z': 35,
                        '-': 36}
        return convert_dict.get(c, 99)
   

    @staticmethod
    def change_int(i):
        i = str(i)
        convert_dict = {'0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5','6': '6', '7': '7', '8': '8', '9': '9', '10': 'A', '11': 'B', '12': 'C', '13': 'D', '14': 'E', '15': 'F', '16': 'G', '17': 'H','18': 'J', '19': 'K', '20': 'L', '21': 'M', '22': 'N', '23': 'P', '24': 'Q', '25': 'R', '26': 'S', '27': 'T', '28': 'U', '29': 'V','30': 'W', '31': 'X', '32': 'Y', '33': 'Z'}
        return convert_dict.get(i, '0')

    @staticmethod
    def transL3D(tft_chip_id):
        t1 = tft_chip_id[:6]
        t2 = tft_chip_id[13:15]
        x1 = tft_chip_id[6:10]
        x2 = tft_chip_id[11:13]
        i1 = int(x1)
        i2 = int(x2)
        m1 = i1 // 34
        n1 = i1 % 34
        m11 = m1 // 34
        n11 = m1 % 34
        n2 = i2 % 34
        auo_chip_id = t1 + chiputilbox.change_int(m11) + chiputilbox.change_int(n11) + \
            chiputilbox.change_int(n1) + chiputilbox.change_int(n2) + t2
        return auo_chip_id
    @staticmethod
    def transL3D17(tft_chip_id):
        t1 = tft_chip_id[:6]
        t2 = tft_chip_id[15:17]
        x1 = tft_chip_id[7:10]  # 7~10 user 0~Z replace
        x2 = tft_chip_id[11:13]  # 12~13 user 0~Z replace
        x3 = tft_chip_id[14:15]  # 14~15 user 0~Z replace
        i1 = int(x1)
        i2 = int(x2)
        i3 = int(x3)
        m1 = i1 // 34
        n1 = i1 % 34
        m11 = m1 // 34
        n11 = m1 % 34
        n2 = i2 % 34
        n3 = i3 % 34
        # _auo_chip_id = t1 + ChipUtil.changeInt(m11) + ChipUtil.changeInt(n11) +
        # ChipUtil.changeInt(n1) + ChipUtil.changeInt(n2) + ChipUtil.changeInt(n3) + t2;
        auo_chip_id = t1 + chiputilbox.change_int(m11) + chiputilbox.change_int(n11) + \
            chiputilbox.change_int(n2) + chiputilbox.change_int(n3) + t2
        return auo_chip_id
    @staticmethod
    def transL5D(tft_chip_id):
        t1 = tft_chip_id[:7]  # 1-7
        t2 = tft_chip_id[13:15]  # 14-15
        x1 = tft_chip_id[7:10]  # 8-10
        x2 = tft_chip_id[11:13]  # 12-13
        i1 = int(x1)
        i2 = int(x2)
        m1 = i1 // 34
        n1 = i1 % 34
        n2 = i2 % 34
        auo_chip_id = t1 + chiputilbox.change_int(m1) + \
            chiputilbox.change_int(n1) + chiputilbox.change_int(n2) + t2
        return auo_chip_id
    @staticmethod
    def transL6B(tft_chip_id):
        t1 = tft_chip_id[:9]  # 1-9
        t2 = tft_chip_id[13:15]  # 14-15
        x1 = tft_chip_id[11:13]  # 12-13
        i1 = int(x1)
        n1 = i1 % 34
        auo_chip_id = t1 + chiputilbox.change_int(n1) + t2
        return auo_chip_id
    @staticmethod
    def transL5D16(tft_chip_id):
        t1 = tft_chip_id[:5]  # 1-5
        t2 = tft_chip_id[6:7]  # 7
        t3 = tft_chip_id[15:16]  # 16
        x1 = tft_chip_id[7:10]  # 8-10
        x2 = tft_chip_id[11:13]  # 12-13
        x3 = tft_chip_id[13:15]  # 14-15
        i1 = int(x1)
        i2 = int(x2)
        i3 = int(x3)
        m1 = i1 // 34
        n1 = i1 % 34
        n2 = i2 % 34
        n3 = i3 % 34
        auo_chip_id = t1 + "W" + t2 + chiputilbox.change_int(m1) + \
            chiputilbox.change_int(n1) + chiputilbox.change_int(n2) + \
            chiputilbox.change_int(n3) + t3
        return auo_chip_id
    @staticmethod
    def transL3C16(tft_chip_id):
        s1_6 = tft_chip_id[:6]  # do nothing
        s7_10 = tft_chip_id[6:10]  # trans
        s12_13 = tft_chip_id[11:13]  # trans
        s13_14 = tft_chip_id[13:14]  # do nothing
        s14_16 = tft_chip_id[14:16]  # do nothing

        i7_10 = int(s7_10)
        i14_16 = int(s14_16)

        bit3 = i7_10 // (34 * 34)
        bit2 = (i7_10 - (bit3 * 34 * 34)) // 34
        bit1 = i7_10 % 34
        c7 = chiputilbox.change_int(bit3)
        c8 = chiputilbox.change_int(bit2)
        c9 = chiputilbox.change_int(bit1)

        bit14 = i14_16 % 34
        c14 = chiputilbox.change_int(bit14)

        i12_13 = int(s12_13)
        c10 =  chiputilbox.change_int(i12_13)

        auo_chip_id = s1_6 + c7 + c8 + c9 + c10 + s13_14 + c14

        return auo_chip_id
    @staticmethod
    def transL3D16(tft_chip_id):
        s1_6 = tft_chip_id[:6]  # do nothing
        s7_10 = tft_chip_id[6:10]  # trans
        s12_13 = tft_chip_id[11:13]  # trans
        s13_16 = tft_chip_id[13:16]  # do nothing

        i7_10 = int(s7_10)
        bit3 = i7_10 // (34 * 34)
        bit2 = (i7_10 - (bit3 * 34 * 34)) // 34
        bit1 = i7_10 % 34
        c7 = chiputilbox.change_int(bit3)
        c8 = chiputilbox.change_int(bit2)
        c9 = chiputilbox.change_int(bit1)

        i12_13 = int(s12_13)
        c10 = chiputilbox.change_int(i12_13)

        # auo_chip_id = s1_6 + c7 + c8 + c9 + s13_16;
        auo_chip_id = s1_6 + c8 + c9 + c10 + s13_16

        return auo_chip_id
    @staticmethod
    def transL3C16(tft_chip_id):
        s1_6 = tft_chip_id[:6]  # do nothing
        s7_10 = tft_chip_id[6:10]  # trans
        s12_13 = tft_chip_id[11:13]  # trans
        s14_16 = tft_chip_id[14:16]  # do nothing

        i7_10 = int(s7_10)
        bit3 = i7_10 // (34 * 34)
        bit2 = (i7_10 - (bit3 * 34 * 34)) // 34
        bit1 = i7_10 % 34
        c7 = chiputilbox.change_int(bit3)
        c8 = chiputilbox.change_int(bit2)
        c9 = chiputilbox.change_int(bit1)

        i12_13 = int(s12_13)
        c10 = chiputilbox.change_int(i12_13)

        auo_chip_id = s1_6 + c7 + c8 + c9 + c10 + s14_16

        return auo_chip_id
    @staticmethod
    def transL5D17(tft_chip_id):
        t1 = tft_chip_id[:5]  # 1-5
        t2 = tft_chip_id[6:7]  # 7
        t3 = tft_chip_id[15:17]  # 16-17
        x1 = tft_chip_id[7:10]  # 8-10
        x2 = tft_chip_id[11:13]  # 12-13
        i1 = int(x1)
        i2 = int(x2)
        m1 = i1 // 34
        n1 = i1 % 34
        n2 = i2 % 34
        auo_chip_id = t1 + "W" + t2 + chiputilbox.change_int(m1) + \
            chiputilbox.change_int(n1) + chiputilbox.change_int(n2) + t3
        return auo_chip_id