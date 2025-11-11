# import json
# from typing import Iterable
# from collections import OrderedDict
# from scrapy import Spider, Request
#
#
# class RecipesSpider(Spider):
#     name = "breakfast"
#     headers = {
#         'Upgrade-Insecure-Requests': '1',
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
#         'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
#         'sec-ch-ua-mobile': '?0',
#         'sec-ch-ua-platform': '"Windows"',
#     }
#
#     custom_settings = {
#         'CONCURRENT_REQUESTS': 8,
#         'FEEDS': {
#             'output/simply_recipes_breakfast.csv': {
#                 'format': 'csv',
#                 'overwrite': True,
#                 'encoding': 'utf8',
#             }
#         }
#     }
#
#     def start_requests(self) -> Iterable[Request]:
#         start_url = 'https://www.simplyrecipes.com/breakfast-recipes-5091541'
#         yield Request(url=start_url, headers=self.headers, callback=self.parse)
#
#     def parse(self, response, **kwargs):
#         yield from self.parse_recipe_listing(response)
#         category_links = response.css('div.mntl-taxonomysc-child-block__links a::attr(href)').getall()[:3]
#
#         for url in category_links:
#             if url and url.startswith("http"):
#                 yield Request(url, callback=self.parse_recipe_listing, headers=self.headers)
#
#     def parse_recipe_listing(self, response):
#         recipe_links = response.css('#mntl-taxonomysc-article-list_1-0 a ::attr(href)').getall()
#
#         for recipe_url in recipe_links:
#             if recipe_url and recipe_url.startswith("http"):
#                 yield Request(recipe_url, callback=self.parse_recipe_details, headers=self.headers)
#
#     def parse_recipe_details(self, response):
#         item = OrderedDict()
#         item['page_url'] = response.url
#         item['og:title'] = response.css('meta[property="og:title"]::attr(content)').get() or ''
#         item['og:description'] = response.css('meta[property="og:description"]::attr(content)').get() or ''
#         item['og:site_name'] = response.css('meta[property="og:site_name"]::attr(content)').get() or ''
#         item['Title'] = response.css('.heading__title::text').get() or ''
#         item['Author'] = ' and '.join(set(response.css('a.mntl-attribution__item-name::text').getall()))
#         item['Published Date'] = response.css('div.mntl-attribution__item-date::text').re_first(r'(?:Updated|Published)\s+(.+)')
#
#         labels = response.css('.meta-text__label::text').getall()
#         values = response.css('.meta-text__data::text').getall()
#         title_recipe_card = response.css('title::text').get() or ''
#         recipe_block = [f"{label.strip()}: {value.strip()}" for label, value in zip(labels, values)]
#         item['Description'] = f"{title_recipe_card}\n{'; '.join(recipe_block)}"
#
#         item['Ingredients'] = ', '.join(response.css('ul.structured-ingredients__list li').xpath('normalize-space(.)').getall())
#
#         rows = response.css('table.nutrition-info__table tr')
#         nutrition_facts = {r.css('td::text')[1].get(): r.css('td::text')[0].get().strip() for r in rows if len(r.css('td::text')) >= 2}
#         item['Nutrition Facts'] = ', '.join(f"{k}: {v}" for k, v in nutrition_facts.items())
#
#         item['Reviews'] =  self.extract_reviews(response) #'; '.join(f"{r['name']}: {r['review']}" for r in reviews)
#
#         yield item
#
#     def extract_reviews(self, response):
#         ld_json = response.css('script[type="application/ld+json"]::text').get()
#         if not ld_json:
#             return []
#
#         try:
#             data = json.loads(ld_json)
#         except json.JSONDecodeError:
#             return []
#
#         reviews = []
#         if isinstance(data, list):
#             for item in data:
#                 if isinstance(item, dict) and 'review' in item:
#                     reviews = item['review']
#                     break
#         elif isinstance(data, dict) and 'review' in data:
#             reviews = data['review']
#
#         formatted = [{'name': r.get('author', {}).get('name', ''), 'review': r.get('reviewBody', '')}
#             for r in reviews if isinstance(r, dict) and r.get('reviewBody')
#         ]
#         return formatted
from .base_spider import BaseRecipesSpider

class BreakfastSpider(BaseRecipesSpider):
    name = "breakfast"
    start_url = 'https://www.simplyrecipes.com/breakfast-recipes-5091541'
    max_categories = ['https://www.simplyrecipes.com/healthy-breakfast-recipes-5091517',
                      'https://www.simplyrecipes.com/easy-breakfast-recipes-5091519',
                      'https://www.simplyrecipes.com/quick-breakfast-recipes-5091518']


    # custom_settings = {
    #     'CONCURRENT_REQUESTS': 8,
    #     'FEEDS': {
    #         'output/simply_recipes_breakfast.csv': {
    #             'format': 'csv',
    #             'overwrite': True,
    #             'encoding': 'utf8',
    #         }
    #     }
    # }

