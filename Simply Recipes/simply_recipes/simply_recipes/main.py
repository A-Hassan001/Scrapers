import sys
import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from spiders.breakfast import BreakfastSpider
from spiders.snacks import SnacksSpider
from spiders.lunch import LunchSpider
from spiders.dinner import DinnerSpider
from spiders.dessert import DessertSpider

def run_all_spiders():
    process = CrawlerProcess(get_project_settings())

    process.crawl(BreakfastSpider)
    process.crawl(SnacksSpider)
    process.crawl(LunchSpider)
    process.crawl(DinnerSpider)
    process.crawl(DessertSpider)

    process.start()

if __name__ == '__main__':
    run_all_spiders()
