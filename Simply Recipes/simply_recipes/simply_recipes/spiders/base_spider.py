import json
from typing import Iterable
from collections import OrderedDict
from scrapy import Spider, Request


class BaseRecipesSpider(Spider):
    headers = {
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    name='base'

    @classmethod
    def get_custom_settings(cls):
        return {
            'CONCURRENT_REQUESTS': 8,
            'FEEDS': {
                f'output/simply_recipes_{cls.name}.csv': {
                    'format': 'csv',
                    'overwrite': True,
                    'encoding': 'utf8',
                }
            }
        }

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.name != 'base':
            cls.custom_settings = cls.get_custom_settings()

    def start_requests(self) -> Iterable[Request]:
        yield Request(url=self.start_url, headers=self.headers, callback=self.parse)

    def parse(self, response, **kwargs):
        yield from self.parse_recipe_listing(response)
        category_links = self.max_categories #response.css('div.mntl-taxonomysc-child-block__links a::attr(href)').getall()[:self.max_categories]

        for url in category_links:
            if url and url.startswith("http"):
                yield Request(url, callback=self.parse_recipe_listing, headers=self.headers)

    def parse_recipe_listing(self, response):
        recipe_links = response.css('#mntl-taxonomysc-article-list_1-0 a::attr(href)').getall()
        for recipe_url in recipe_links:
            if recipe_url and recipe_url.startswith("http"):
                yield Request(recipe_url, callback=self.parse_recipe_details, headers=self.headers)

    def parse_recipe_details(self, response):
        item = OrderedDict()
        item['page_url'] = response.url
        item['og:title'] = response.css('meta[property="og:title"]::attr(content)').get('')
        item['og:description'] = response.css('meta[property="og:description"]::attr(content)').get('')
        item['og:site_name'] = response.css('meta[property="og:site_name"]::attr(content)').get('')
        item['Title'] = response.css('.heading__title::text').get('')
        item['Author'] = (' and '.join(set(response.css('a.mntl-attribution__item-name::text').getall()))
                          or response.css('span.mntl-attribution__item-name ::text').get(default='').strip())

        item['Published Date'] = response.css('div.mntl-attribution__item-date::text').re_first(r'(?:Updated|Published)\s+(.+)')

        labels = response.css('.meta-text__label::text').getall()
        values = response.css('.meta-text__data::text').getall()
        title_recipe_card = response.css('title::text').get('')
        recipe_block = [f"{label.strip()}: {value.strip()}" for label, value in zip(labels, values)]
        item['Description'] = f"{title_recipe_card}\n{'; '.join(recipe_block)}"

        item['Ingredients'] = self.get_ingredients(response)    #', '.join(response.css('ul.structured-ingredients__list li').xpath('normalize-space(.)').getall())

        rows = response.css('table.nutrition-info__table tr')
        nutrition_facts = {r.css('td::text')[1].get(): r.css('td::text')[0].get().strip() for r in rows if len(r.css('td::text')) >= 2}
        item['Nutrition Facts'] = ', '.join(f"{k}: {v}" for k, v in nutrition_facts.items())

        item['Reviews'] = self.extract_reviews(response)
        yield item

    def get_ingredients(self, response):
        # 1. Try #ingredient-list_1-0 first
        ingredients = response.css('#ingredient-list_1-0 li').xpath('normalize-space(.)').getall()
        if ingredients:
            return ', '.join(ingredients)

        # 2. Try ul.structured-ingredients__list
        ingredients = response.css('ul.structured-ingredients__list li').xpath('normalize-space(.)').getall()
        if ingredients:
            return ', '.join(ingredients)

        # 3. Try <p><strong>INGREDIENTS</strong></p> followed by <p> (with <br/>)
        strong = response.xpath(
            '//p[strong[contains(translate(., "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "ingredients")]]')
        for s in strong:
            next_p = s.xpath('following-sibling::p[1]')
            p_text = next_p.xpath('.//text()').getall()
            p_text = [t.strip() for t in p_text if t.strip()]
            if p_text:
                return ', '.join(p_text)

        # 4. Try "How to Make" fallback
        h2 = response.xpath(
            '//h2[contains(translate(., "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "how to make")]')
        for heading in h2:
            ul = heading.xpath('following-sibling::*[self::ul or (self::div and ul)][1]')
            li_texts = ul.xpath('.//li//text()').getall()
            li_texts = [text.strip() for text in li_texts if text.strip()]
            if li_texts:
                return ', '.join(li_texts)

        return ''

    # def get_ingredients(self, response):
    #     ingredients = response.css('ul.structured-ingredients__list li').xpath('normalize-space(.)').getall()
    #     if ingredients:
    #         Ingredients = ', '.join(ingredients)
    #         return Ingredients
    #     else:
    #         # Fallback to secondary method
    #         h2 = response.css('h2:contains("How ")')
    #         ul = h2.xpath('following-sibling::ul[1]')
    #         li_texts = ul.xpath('.//li/text()').getall()
    #         result = ', '.join([text.strip() for text in li_texts])
    #         return result

    # def get_ingredients(self, response):
    #     # Try primary method
    #     ingredients = response.css('ul.structured-ingredients__list li').xpath('normalize-space(.)').getall()
    #     if ingredients:
    #         return ', '.join(ingredients)
    #
    #     # Secondary: Find <h2> with "How" and get next <ul>
    #     h2 = response.css('h2:contains("How ")')
    #     ul = h2.xpath('following-sibling::ul[1]')
    #     li_texts = ul.xpath('.//li//text()').getall()
    #     li_texts = [text.strip() for text in li_texts if text.strip()]
    #     if li_texts:
    #         return ', '.join(li_texts)
    #
    #     # Third fallback: Check for <p> mentioning "you'll need" followed by <ul>
    #     need_paragraphs = response.xpath("//p[contains(text(), \"you'll need\") or contains(text(), 'you will need')]")
    #     if need_paragraphs:
    #         for p in need_paragraphs:
    #             ul = p.xpath('following-sibling::*[self::ul or self::div/ul][1]')
    #             li_texts = ul.xpath('.//li//text()').getall()
    #             li_texts = [text.strip() for text in li_texts if text.strip()]
    #             if li_texts:
    #                 return ', '.join(li_texts)
    #
    #     # Final fallback: Find any generic <ul> that appears before the first <ol>
    #     first_ol = response.xpath('//ol[1]')
    #     generic_uls = response.xpath('//ul')
    #     for ul in generic_uls:
    #         if first_ol and ul.root is not None and ul.root.sourceline < first_ol.root.sourceline:
    #             li_texts = ul.xpath('.//li//text()').getall()
    #             li_texts = [text.strip() for text in li_texts if text.strip()]
    #             if li_texts:
    #                 return ', '.join(li_texts)
    #
    #     return ''  # Nothing found

    # def get_ingredients(self, response):
    #     # 1. Try primary structured ingredients
    #     ingredients = response.css('ul.structured-ingredients__list li').xpath('normalize-space(.)').getall()
    #     if ingredients:
    #         return ', '.join(ingredients)
    #
    #     # 2. Try "How" section fallback
    #     h2 = response.css('h2:contains("How ")')
    #     ul = h2.xpath('following-sibling::ul[1]')
    #     li_texts = ul.xpath('.//li//text()').getall()
    #     li_texts = [text.strip() for text in li_texts if text.strip()]
    #     if li_texts:
    #         return ', '.join(li_texts)
    #
    #     # 3. Check <p> elements that hint at ingredients (like "you'll need")
    #     need_paras = response.xpath(
    #         "//p[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), \"you'll need\") or contains(., 'you will need')]")
    #     for p in need_paras:
    #         # Get the next <ul> or <div><ul></ul></div>
    #         ul = p.xpath('following-sibling::*[self::ul or (self::div and ul)][1]')
    #         if ul:
    #             li_texts = ul.xpath('.//li//text()').getall()
    #             li_texts = [text.strip() for text in li_texts if text.strip()]
    #             if li_texts:
    #                 return ', '.join(li_texts)
    #
    #     # 4. Fallback: first <ul> before instructions (<ol>)
    #     first_ol = response.xpath('//ol[1]')
    #     uls = response.xpath('//ul')
    #     for ul in uls:
    #         if not first_ol or ul.root.sourceline < first_ol.root.sourceline:
    #             li_texts = ul.xpath('.//li//text()').getall()
    #             li_texts = [text.strip() for text in li_texts if text.strip()]
    #             if li_texts:
    #                 return ', '.join(li_texts)
    #
    #     return ''

    def extract_reviews(self, response):
        ld_json = response.css('script[type="application/ld+json"]::text').get()
        if not ld_json:
            return []

        try:
            data = json.loads(ld_json)
        except json.JSONDecodeError:
            return []

        reviews = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and 'review' in item:
                    reviews = item['review']
                    break
        elif isinstance(data, dict) and 'review' in data:
            reviews = data['review']

        return [
            {'name': r.get('author', {}).get('name', ''), 'review': r.get('reviewBody', '')}
            for r in reviews if isinstance(r, dict) and r.get('reviewBody')
        ]
