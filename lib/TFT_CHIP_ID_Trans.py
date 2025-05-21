def generate_chip_id_list(sheet_id, array_lot_id, sitecode, layout_label_list):
    try:
        layout_label = layout_label_list.split("-")
        chip_id_list = []

        if (len(sheet_id.strip()) in [6, 7, 11, 12, 15] and 
            sitecode.strip() in ["AUHC","AUHY", "AUHL", "AULT", "AUTC", "AUST", "AUSZ", "AUSJ", "AUXM", "AUKS"]):
            for label in layout_label:
                if len(label) == 1:
                    chip_id_list.append(sheet_id.strip()[:6] + label)
                    print("chip_id_list ===> " + str(chip_id_list) + " && layout_label===>" + str(layout_label) + " && sitecode===>" + sitecode) 
                else: #20241210 Perry
                    if len(sheet_id.strip()) == 6:
                        chip_id_list.append(sheet_id.strip()[:6] + label)
                        print("chip_id_list ===> " + str(chip_id_list) + " && layout_label===>" + str(layout_label) + " && sitecode===>" + sitecode) 
                    elif len(sheet_id.strip()) == 7:
                        chip_id_list.append(array_lot_id.strip()[:3] + sheet_id.strip()[:6] + label)
                        print("chip_id_list ===> " + str(chip_id_list) + " && layout_label===>" + str(layout_label) + " && sitecode===>" + sitecode)     
                    elif len(sheet_id.strip()) >= 9:
                        chip_id_list.append(sheet_id.strip()[:9] + label)
                        print("chip_id_list ===> " + str(chip_id_list) + " && layout_label===>" + str(layout_label) + " && sitecode===>" + sitecode)    
                        

        elif (len(sheet_id.strip()) == 9 and 
              sitecode.strip() in ["AUHL", "AULT", "AUTC", "AUST", "AUSZ", "AUSJ", "AUXM", "AUKS"]):
            for label in layout_label:
                if len(label) == 1:
                    chip_id_list.append(sheet_id.strip()[3:9] + label)
                    print("chip_id_list ===> " + str(chip_id_list) + " && layout_label===>" + str(layout_label) + " && sitecode===>" + sitecode) 
                else:
                    chip_id_list.append(sheet_id.strip() + label)
                    print("chip_id_list ===> " + str(chip_id_list) + " && layout_label===>" + str(layout_label) + " && sitecode===>" + sitecode) 

        elif len(sheet_id.strip()) == 10 and sitecode.strip() == "AUKS":
            for label in layout_label:
                if len(label) == 3:
                    chip_id_list.append(sheet_id.strip()[:9] + label)
                    print("chip_id_list ===> " + str(chip_id_list) + " && layout_label===>" + str(layout_label) + " && sitecode===>" + sitecode) 

        elif ((sitecode.strip() in ["AULK" , "AUHY", "AUSZ", "AUSJ", "AUXM"] and len(sheet_id) > 10)):
            for label in layout_label:
                chip_id_list.append(sheet_id.strip()[:10] + "T" + sheet_id.strip()[11:13] + label)
                print("chip_id_list ===> " + str(chip_id_list) + " && layout_label===>" + str(layout_label) + " && sitecode===>" + sitecode) 

        elif len(sheet_id.strip()) == 13 and sitecode.strip() == "AUHC":
            for label in layout_label:
                chip_id_list.append(sheet_id.strip()[:13].replace("-", "A") + label)
                print("chip_id_list ===> " + str(chip_id_list) + " && layout_label===>" + str(layout_label) + " && sitecode===>" + sitecode) 

        return chip_id_list

    except Exception as e:
        raise Exception(f"The sheet_id is not the effective form.===> {sheet_id}; {layout_label_list}")

