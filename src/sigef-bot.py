from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.options import Options

import time

#install the firefox WITHOUT SNAP

driver = webdriver.Firefox()

#driver.get('https://sigef.incra.gov.br/')
#time.sleep(1)

driver.get('https://sso.acesso.gov.br/authorize?response_type=code&client_id=sigef.incra.gov.br&redirect_uri=https%3A%2F%2Fsigef.incra.gov.br%2F&scope=openid+email+profile+govbr_confiabilidades+govbr_recupera_certificadox509&state=zsT07dMcXdkRdTe5V0PIDDN2BRAGoJ')
time.sleep(1)

driver.find_element(By.XPATH, '//*[@id="accountId"]').send_keys('22068769816')
time.sleep(1)

driver.find_element(By.XPATH, '//*[@id="enter-account-id"]').click()
time.sleep(1)

driver.find_element(By.XPATH, '//*[@id="password"]').send_keys('Senha@123')
time.sleep(1)

driver.quit()