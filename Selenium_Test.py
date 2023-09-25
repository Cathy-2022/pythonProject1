import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import os
import pandas as pd
from lxml import etree
os.environ['PATH'] += r"C:/SeleniumDrivers"
driver = webdriver.Chrome()
driver.get("http://10.222.10.17/BI/index.php/login")
driver.find_element(By.ID, "user").send_keys("TABCBARO")
driver.find_element(By.ID, "password").send_keys("C7E8F480C70F246F")
driver.find_element(By.ID, "registration").send_keys("22F9A6C70AE4BE13")
driver.find_element(By.ID, "login-click").click()
time.sleep(5)
driver.get("http://10.222.10.17/BI/index.php/filelist#elf_l1_ZWdt")
driver.find_element(By.XPATH, "//*[@id='elfinder']/div[1]/div[3]/div[1]").click()
driver.find_element(By.XPATH, "//*[@id='elfinder']/div[1]/div[3]/div[2]").click()
time.sleep(10)


