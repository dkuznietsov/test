# Databricks notebook source
import sys

# 添加 lib 目錄到 Python 路徑
sys.path.append("/dbfs/FileStore/lib")

# 測試導入 lib.rrntool
try:
    from lib.rrntool import RrnToolBox, RrnException
    print("成功導入 lib.rrntool")
except ModuleNotFoundError as e:
    print(f"導入失敗: {e}")
