class Echo:
  def __init__(self):
    self.version = "1.1.0"
    self.build_date = "2010.02.20"

    self.chip_history = []
    self.chip_version = "1.1.3"
    self.chip_build_date = "2010.02.20"

    self.rrn_history = []
    self.rrn_version = "1.0.7"
    self.rrn_build_date = "2008.08.01"

    self.chip_history.append("  1.0.0: Initial Version \n")
    self.chip_history.append("  1.0.7a: Bug Fix -naming for L3D Chip Id number five char <> 'G' and number six char<> 'A~Z' \n")
    self.chip_history.append("  1.1.1: Add old L6B Chip Id number six char='C' \n")
    self.chip_history.append("  1.1.2: Add Chip Exception handle \n")
    self.chip_history.append("  1.1.3: Add L5D Chip Id number five char ='H'")
    self.rrn_history.append("  1.0.0: Initial Version \n")
    self.rrn_history.append("  1.0.7: Add L5I(IVO) tft_chip_id_rrn logic.")

  def print(self):
    print(" Verion : ", self.version)
    print(" Build Date  : ", self.build_date)
    print(" ======ChipTool(Module Unique Chip Id)==============================")
    print(" ChipTool Verion : ", self.chip_version)
    print(" ChipTool Build Date  : ", self.chip_build_date)
    print(" ChipTool Version History : \n" + "\n".join(self.chip_history))
    print(" ===================================================================")
    print(" ======RrnTool(Tft_chip_id_rrn)=====================================")
    print(" RrnTool Verion : ", self.rrn_version)
    print(" RrnTool Build Date  : ", self.rrn_build_date)
    print(" RrnTool Version History : \n" + "\n".join(self.rrn_history))
    print(" ===================================================================")

def main():
    echo = Echo()
    echo.print()

if __name__ == "__main__":
    main()