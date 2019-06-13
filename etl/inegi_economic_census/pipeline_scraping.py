from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options
from time import sleep
import glob
import os

def scraping(product_section, geography):

    fp = webdriver.FirefoxProfile()
    directory = "{}/data_temp".format(os.getcwd())

    fp.set_preference("browser.download.folderList", 2)
    fp.set_preference("browser.download.manager.showWhenStarting",False)
    fp.set_preference("browser.helperApps.alwaysAsk.force", False)
    fp.set_preference("browser.download.dir", directory)
    fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/text,text/plain,text/csv,application/csv,application/download,application/octet-stream")

    options = Options()
    options.headless = True

    firefox_capabilities = DesiredCapabilities.FIREFOX
    firefox_capabilities["marionette"] = True

    driver = webdriver.Firefox(capabilities=firefox_capabilities, firefox_profile=fp, options=options)
    driver.get("https://www.inegi.org.mx/app/saic/default.aspx")


    # Expands Censal Variables Tab
    driver.find_element_by_id("headingVaria").click()

    for value in ["A", "B", "C", "D", "E", "F", "G", "H", "I"]:
        driver.find_element_by_id("P_T0_A{}_DinamicGridGrupoVariables".format(value)).click()
        sleep(3)
        
    # Expands Geography Selector Tab
    driver.find_element_by_id("headingAgeo").click()
    for value in geography:
        driver.find_element_by_id("P_T{}_DinamicGridCoberturaGeografica".format(str(value).zfill(2))).click()
        sleep(3)

    driver.find_element_by_id("headingEco").click()

    driver.find_element_by_id("P{}_DinamicGridActividadEconomica".format(product_section)).click()
    sleep(3)
    activity = driver.find_element_by_id("D{}_DinamicGridActividadEconomica".format(product_section))
    all_children = activity.find_elements_by_css_selector('a[title*="Expande tema"][id$="_DinamicGridActividadEconomica"]')

    for child in all_children:
        child_id = child.get_property("id")
        child.click()
        sleep(3)


        sub_activity = driver.find_element_by_id("D{}".format(child_id[1:]))
        all_children_sub_activity = sub_activity.find_elements_by_css_selector('a[title*="Expande tema"][id$="_DinamicGridActividadEconomica"]')

        for sub_child in all_children_sub_activity:

            sub_child_id = sub_child.get_property("id")
            sub_child.click()
            sleep(3)

            expand_list = driver.find_element_by_id("D{}".format(sub_child_id[1:]))
            asdf = expand_list.find_elements_by_css_selector('a[id^="P_T"]')

            for item in asdf:
                item.click()

    # Descarga los datos en CSV
    driver.find_element_by_id("btnConsulta").click()
    sleep(30)
    driver.find_element_by_id("BtnsExporta").click()
    sleep(10)
    driver.find_element_by_id("csv").click()
    sleep(60)

entities = [[str(j) for j in range(1 + 8*i, 9 + 8*i)] for i in range(2, 3)]
products = ["11", "21", "22", "23", "31", "43", "46", "48", "51", "52", "53", "54", "55", "56", "61", "62", "71", "72", "81", "SC"]

for entity in entities:
    for product in products:
        print("PRODUCT: {}\tGEO: {}".format(product, ",".join(entity)))
        scraping(product, entity)
        sleep(10)