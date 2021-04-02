# Scraper
Web Scraper package, fork from https://github.com/epyonchen/Scraper

## Document
  ### Set up
  1. Use command line and go to your target folder  
    ```cd c:\xx\your_folder```
  2. Pull repository to your local machine  
    ```git clone https://github.com/JLL-Benson/Scraper.git```  
  3. Use pycharm to open the project, pycharm may help you set up environment in your first time opening the project. If you are not using pycharm as IDE, go to step 4.
  4. Install required package  
    ```pip install -r requirements.txt```
  5. Download and install firefox version 65 or lower
  6. Download latest geckodriver from this [link](https://github.com/mozilla/geckodriver/releases), put geckodriver in your_python_path/Scripts (e.g. c:\python\Scripts). If you are using virtual environment, put geckodriver in virtual_path/Scripts (e.g. c:\pycharmprojects\Scrapers\venv\Scripts)
    
  ### Modules
  1. handlers: core modules of scraping, api requesting and database connecting 
      - scrapers: core scraping module, defines basic scrapers to handle single or two layers and workflow of execution
        - class Scraper: one step scraper
        - search(): query one link and return bs4 object
        - get_item_list(): traverse item list, in most common case, property list, need to be overwritten when instantiating according to target page's format
        - format_df(): format result dataframe into table structure in database
        - run(): main execution process, iterate pages for items
        - class TwoStepScraper: inheritance of Scraper, two steps Scraper
        - get_item_detail(): traverse detail list of single, mainly referring to office units in one property, need to be overwritten when instantiating according to target page's format
      - diandianzu, haozu: inheritance of scrapers, main modules of scraping rental price and other office information from diandianzu and haozu
        - class Diandianzu, Haozu: inheritance of TwoStepScraper
        - get_item_info(): get item(property) information from item detail(property detail) page
      - default_api: core api requesting module, defines basic api related functions
        - _api_keys: required parameters of each api type in one api platform, key-value pairs as 'api type': [list of required parameters]
        - _default_kwargs: common parameters of all api types in one api platform, key-value pairs as 'parameter name': default value
        - _alter_kwargs: alternative parameter name of universal parameters in different platforms in pre-built query, including sign(secrect key), keyword(query keyword), page, lat, lon. key-value pairs as 'pre-built name in this module': 'actual name in targeted api platform'
        - update_parameters(): update parameters which ar
      - baidu_api, amap_api: inheritance of default_api, implement or overwrite functions and default class parameters accrodingly

  ### Tables and views
