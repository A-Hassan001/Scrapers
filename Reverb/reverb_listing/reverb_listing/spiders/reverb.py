import re
import os
import csv
import json
import requests

from typing import Any
from datetime import datetime
from itertools import zip_longest
from  scrapy import Request, Spider
from collections import OrderedDict



class ReverbSpider(Spider):
    name = "reverb"
    start_urls = ["https://gql.reverb.com/graphql"]
    base_url = "https://reverb.com"

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.columns = ['Handle','Title','Body (HTML)','Vendor','Type','Tags','Published','Option1 Name','Option1 Value','Option2 Name',
                    'Option2 Value','Option3 Name','Option3 Value','Variant SKU','Variant Grams','Variant Inventory Tracker','Variant Inventory Qty',
                    'Variant Inventory Policy','Variant Fulfillment Service','Variant Price','Variant Compare At Price','Variant Requires Shipping',
                    'Variant Taxable','Variant Barcode','Image Src','Image Position','Image Alt Text','Variant Image','Gift Card','SEO Title',
                    'SEO Description','Google Shopping / Google Product Category','Google Shopping / Gender','Google Shopping / Age Group',
                    'Google Shopping / MPN','Google Shopping / AdWords Grouping','Google Shopping / AdWords Labels','Google Shopping / Condition',
                    'Google Shopping / Custom Product','Google Shopping / Custom Label 0','Google Shopping / Custom Label 1',
                    'Google Shopping / Custom Label 2','Google Shopping / Custom Label 3','Google Shopping / Custom Label 4','Variant Weight Unit',
                    'Variant Tax Code','Cost per item','Status','URL']

        self.output_file_name = f'output/reverb Shopify  {datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
        self.api_key = open("proxy/proxy.txt").read().strip() 
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en',
            'content-type': 'application/json',
            'origin': 'https://reverb.com',
            'referer': 'https://reverb.com/',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'x-context-id': '1562f3f2-6dd2-4323-8d51-ccd474319f18',
            'x-display-currency': 'USD',
            'x-experiments': 'proximity_features',
            'x-item-region': 'XX',
            'x-request-id': '994f65c97b3b4121',
            'x-reverb-app': 'REVERB',
            'x-reverb-device-info': '{"platform":"web","app_version":0,"platform_version":null}',
            'x-reverb-user-info': '{"mpid":"-7576526006919400599","session_id":"91E709CD-6DC4-4AEF-9FD0-6E5406A6AE52","device_id":"5ff2333c-9d9c-41d8-6fce-cf988ded9208","user_id":null,"ra":false,"is_bot":false}',
            'x-secondary-user-enabled': 'false',
            'x-session-id': 'd00e185f-f1bb-4505-92d6-7a1eb356b290',
            'x-shipping-region': 'PK',
        }
        self.detail_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            # 'cookie': 'timezone=Asia/Karachi; timezone_offset=5; reverb_user_shipping_region=PK; reverb_user_currency=USD; reverb_user_locale=en; reverb_user_country_code=PK; lantern=d08131a6-b4bd-46da-8e8b-50a23331c162; _gcl_au=1.1.117393277.1761536236; _gid=GA1.2.1602188711.1761536247; _fbp=fb.1.1761536246873.915499797590819939; google_cid=1359575423.1761536238; reverb_mparticle_user_info={%22mpid%22:%22-7576526006919400599%22%2C%22session_id%22:%2291E709CD-6DC4-4AEF-9FD0-6E5406A6AE52%22%2C%22device_id%22:%225ff2333c-9d9c-41d8-6fce-cf988ded9208%22}; elog-analytics-2={%22lastSeen%22:1761542219559%2C%22id%22:%221562f3f2-6dd2-4323-8d51-ccd474319f18%22%2C%22sessionID%22:%22d00e185f-f1bb-4505-92d6-7a1eb356b290%22}; csrf_token=wVNNLjJALseq56FPuDdSOSEr45qY2fgKwI1KndlDdTc1gmAo6rCGuE4n9V1U0kHmYNbvJGD1qGlcZvHea9P9DA; _reverb_session=b1JOcFNvWk5JeEFyYjhpR1prWGNJcUVMWVdFamV5VVFESTVzeVYza3ZMVjZkYUJrSE9ubVFCa1ZVM052VEZwbUs3bU1GUkpaZHRYVWhMTDFuc0V0SmZNSHRxSi9mdWIrbUp4bmZrK0h2VTVra3ZtNSszRUtaK3ZkMXYyVWR6ODBhaXJnZWdaKzJCb0FFa0NFK3QzU0MwSW9KSGJZRWJkL3VaY1g3TmZZQ21jYjAwbDczVmxhbnl6Ykp4dGtUcGZxU1QxVGNQWFJpcDhzKzFqRG5JajlES0JjQTBqU0grQWc4aml1OG9BU1JtakVlajBnMXd5ckFiZ01EcDZQY0ovanB6Q3YzVjBVaFpnZzdmN21XcWtuUFJVR2pDdE5wUjdCNjg1NmlSRXV4VWxlTFRsbXZUajJmNlJxQUw0U3g5b2p3Z1BlZWoyRjgvMHRzZTRGSnhSaEt3Rjdma1ZnV3JkcEt2anJMa0ZzSGRraTFFR0huazdZNE5vQ09SdmtBbWhGLS15YXZGYVpIY21FVzh4cUFzY0ZtTXJ3PT0%3D--b117a5f84fd862ef9652e323d9587a9db8f2726c; __cf_bm=tR6NLSgLkg8wXUYy49Vp2ZylhVK1EpZK2lRjEA8sybE-1761543246-1.0.1.1-ZfbJo_D.B0Miej7ER1KxIDJrT0O.i7sJj98Jq2aD9Y2ZfMFx5oUFwhq5OTmVJ0H5OLiwJjPt1wruYa9o0QZxCT3kVxancjcfifqKMaG.mBs; reverb_page_views=13; _ga_K0VJZSKN23=GS2.1.s1761536237$o1$g1$t1761543252$j53$l0$h0; _ga=GA1.1.1359575423.1761536238',
        }

    def start_requests(self):
        json_data = {
            'operationName': 'Core_Marketplace_CombinedMarketplaceSearch',
            'variables': {
                'inputListings': {
                    'priceMin': '1000',
                    'sortSlug': 'price_with_sale|desc',
                    'categorySlugs': [],
                    'brandSlugs': [],
                    'conditionSlugs': [],
                    'shippingRegionCodes': ['XX'],
                    'showOnlySold': True,
                    'showSold': False,
                    'itemState': [],
                    'itemCity': [],
                    'shopSlug': 'river-city-guitars-llc',
                    'limit': 45,
                    'offset': 45,
                },
                'inputBumped': {
                    'priceMin': '1000',
                    'sortSlug': 'price_with_sale|desc',
                    'shippingRegionCodes': ['XX'],
                    'showOnlySold': True,
                    'showSold': False,
                    'shopSlug': 'river-city-guitars-llc',
                    'limit': 15,
                    'offset': 15,
                    'bumpedOnly': True,
                },
                'inputAggs': {
                    'priceMin': '1000',
                    'sortSlug': 'price_with_sale|desc',
                    'shippingRegionCodes': ['XX'],
                    'showOnlySold': True,
                    'showSold': False,
                    'shopSlug': 'river-city-guitars-llc',
                    'limit': 0,
                    'withAggregations': [
                        'CATEGORY_SLUGS',
                        'BRAND_SLUGS',
                        'CONDITION_SLUGS',
                        'DECADES',
                        'CURATED_SETS',
                        'COUNTRY_OF_ORIGIN',
                    ],
                },
                'shouldntLoadBumps': True,
                'shouldntLoadSuggestions': False,
                'usingListView': False,
                'signalGroups': ['MP_GRID_CARD'],
                'useSignalSystem': False,
            },
            'query': 'query Core_Marketplace_CombinedMarketplaceSearch($inputListings: Input_reverb_search_ListingsSearchRequest, $inputBumped: Input_reverb_search_ListingsSearchRequest, $inputAggs: Input_reverb_search_ListingsSearchRequest, $shouldntLoadBumps: Boolean!, $shouldntLoadSuggestions: Boolean!, $usingListView: Boolean!, $signalGroups: [reverb_signals_Signal_Group], $useSignalSystem: Boolean!) {\n  bumpedSearch: listingsSearch(input: $inputBumped) @skip(if: $shouldntLoadBumps) {\n    listings {\n      _id\n      ...ListingCardFields\n      ...WatchBadgeData\n      ...BumpKey\n      ...ShopFields\n      ...ListViewListings @include(if: $usingListView)\n      signals(input: {groups: $signalGroups}) @include(if: $useSignalSystem) {\n        ...ListingCardSignalsData\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  aggsSearch: listingsSearch(input: $inputAggs) {\n    filters {\n      ...NestedFilter\n      __typename\n    }\n    __typename\n  }\n  listingsSearch(input: $inputListings) {\n    total\n    offset\n    limit\n    suggestedQueries\n    eligibleAutodirects\n    correctedTo\n    correctedFrom\n    listings {\n      _id\n      esScore\n      ...ListingCardFields\n      ...WatchBadgeData\n      ...InOtherCartsCardData @skip(if: $useSignalSystem)\n      ...ShopFields\n      ...ListViewListings @include(if: $usingListView)\n      signals(input: {groups: $signalGroups}) @include(if: $useSignalSystem) {\n        ...ListingCardSignalsData\n        __typename\n      }\n      __typename\n    }\n    fallbackListings {\n      _id\n      ...ListingCardFields\n      ...InOtherCartsCardData @skip(if: $useSignalSystem)\n      ...WatchBadgeData\n      ...ShopFields\n      signals(input: {groups: $signalGroups}) @include(if: $useSignalSystem) {\n        ...ListingCardSignalsData\n        __typename\n      }\n      __typename\n    }\n    suggestions @skip(if: $shouldntLoadSuggestions) {\n      ...MarketplaceSuggestions\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment ListingCardFields on Listing {\n  _id\n  ...ListingForBuyerFields\n  ...WatchBadgeData\n  ...ListingCreateOfferButtonData\n  __typename\n}\n\nfragment ListingForBuyerFields on Listing {\n  _id\n  id\n  title\n  slug\n  listingType\n  make\n  model\n  upc\n  state\n  stateDescription\n  bumped\n  watching\n  soldAsIs\n  usOutlet\n  publishedAt {\n    seconds\n    __typename\n  }\n  condition {\n    displayName\n    conditionSlug\n    conditionUuid\n    __typename\n  }\n  pricing {\n    buyerPrice {\n      display\n      currency\n      amount\n      amountCents\n      __typename\n    }\n    originalPrice {\n      display\n      __typename\n    }\n    ribbon {\n      display\n      reason\n      __typename\n    }\n    __typename\n  }\n  images(\n    input: {transform: "card_square", count: 3, scope: "photos", type: "Product"}\n  ) {\n    source\n    __typename\n  }\n  shipping {\n    shippingPrices {\n      _id\n      shippingMethod\n      carrierCalculated\n      destinationPostalCodeNeeded\n      rate {\n        amount\n        amountCents\n        currency\n        display\n        __typename\n      }\n      __typename\n    }\n    freeExpeditedShipping\n    localPickupOnly\n    localPickup\n    __typename\n  }\n  shop {\n    _id\n    name\n    returnPolicy {\n      usedReturnWindowDays\n      newReturnWindowDays\n      __typename\n    }\n    address {\n      _id\n      locality\n      region\n      country {\n        _id\n        countryCode\n        name\n        __typename\n      }\n      displayLocation\n      __typename\n    }\n    __typename\n  }\n  ...ListingForBuyerShippingFields\n  ...ListingGreatValueData\n  ...ListingCardCPOData\n  __typename\n}\n\nfragment ListingGreatValueData on Listing {\n  _id\n  pricing {\n    buyerPrice {\n      currency\n      amountCents\n      __typename\n    }\n    typicalNewPriceDisplay {\n      amountDisplay\n      __typename\n    }\n    __typename\n  }\n  condition {\n    conditionSlug\n    __typename\n  }\n  priceRecommendation {\n    priceMiddle {\n      amountCents\n      currency\n      __typename\n    }\n    __typename\n  }\n  shop {\n    _id\n    address {\n      _id\n      country {\n        _id\n        countryCode\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  currency\n  __typename\n}\n\nfragment ListingForBuyerShippingFields on Listing {\n  _id\n  shipping {\n    freeExpeditedShipping\n    localPickupOnly\n    shippingPrices {\n      _id\n      shippingMethod\n      carrierCalculated\n      regional\n      destinationPostalCodeNeeded\n      postalCode\n      rate {\n        amount\n        amountCents\n        currency\n        display\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ListingCardCPOData on Listing {\n  _id\n  id\n  certifiedPreOwned {\n    title\n    badgeIconUrl\n    __typename\n  }\n  __typename\n}\n\nfragment ListingCreateOfferButtonData on Listing {\n  id\n  _id\n  state\n  offersEnabled\n  listingType\n  sellerId\n  isBuyerOfferEligible\n  ...mParticleListingFields\n  __typename\n}\n\nfragment mParticleListingFields on Listing {\n  id\n  _id\n  title\n  brandSlug\n  categoryRootUuid\n  make\n  categoryUuids\n  state\n  listingType\n  bumpEligible\n  shopId\n  inventory\n  soldAsIs\n  acceptedPaymentMethods\n  currency\n  usOutlet\n  condition {\n    conditionUuid\n    conditionSlug\n    __typename\n  }\n  categories {\n    _id\n    slug\n    rootSlug\n    __typename\n  }\n  csp {\n    _id\n    id\n    slug\n    brand {\n      _id\n      slug\n      __typename\n    }\n    __typename\n  }\n  pricing {\n    buyerPrice {\n      amount\n      currency\n      amountCents\n      __typename\n    }\n    __typename\n  }\n  publishedAt {\n    seconds\n    __typename\n  }\n  sale {\n    _id\n    id\n    code\n    buyerIneligibilityReason\n    __typename\n  }\n  shipping {\n    shippingPrices {\n      _id\n      shippingMethod\n      carrierCalculated\n      destinationPostalCodeNeeded\n      rate {\n        amount\n        amountCents\n        currency\n        display\n        __typename\n      }\n      __typename\n    }\n    freeExpeditedShipping\n    localPickupOnly\n    localPickup\n    __typename\n  }\n  certifiedPreOwned {\n    title\n    badgeIconUrl\n    __typename\n  }\n  shop {\n    _id\n    address {\n      _id\n      countryCode\n      __typename\n    }\n    returnPolicy {\n      _id\n      newReturnWindowDays\n      usedReturnWindowDays\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment WatchBadgeData on Listing {\n  _id\n  id\n  title\n  sellerId\n  watching\n  __typename\n}\n\nfragment BumpKey on Listing {\n  _id\n  bumpKey {\n    key\n    __typename\n  }\n  __typename\n}\n\nfragment ShopFields on Listing {\n  _id\n  shop {\n    _id\n    address {\n      _id\n      locality\n      countryCode\n      region\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment InOtherCartsCardData on Listing {\n  _id\n  id\n  otherBuyersWithListingInCartCounts\n  __typename\n}\n\nfragment NestedFilter on reverb_search_Filter {\n  name\n  key\n  aggregationName\n  widgetType\n  options {\n    count {\n      value\n      __typename\n    }\n    name\n    selected\n    autoselected\n    paramName\n    setValues\n    unsetValues\n    all\n    optionValue\n    trackingValue\n    subFilter {\n      widgetType\n      options {\n        count {\n          value\n          __typename\n        }\n        name\n        selected\n        autoselected\n        paramName\n        setValues\n        unsetValues\n        all\n        optionValue\n        trackingValue\n        subFilter {\n          widgetType\n          options {\n            count {\n              value\n              __typename\n            }\n            name\n            selected\n            autoselected\n            paramName\n            setValues\n            unsetValues\n            all\n            optionValue\n            trackingValue\n            subFilter {\n              widgetType\n              options {\n                count {\n                  value\n                  __typename\n                }\n                name\n                selected\n                autoselected\n                paramName\n                setValues\n                unsetValues\n                all\n                optionValue\n                trackingValue\n                __typename\n              }\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment MarketplaceSuggestions on reverb_search_SearchResponse_Suggestion {\n  text\n  __typename\n}\n\nfragment ListViewListings on Listing {\n  _id\n  id\n  categoryUuids\n  state\n  shop {\n    _id\n    name\n    slug\n    preferredSeller\n    quickShipper\n    quickResponder\n    address {\n      _id\n      locality\n      region\n      displayLocation\n      country {\n        _id\n        countryCode\n        name\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  seller {\n    _id\n    feedbackSummary {\n      receivedCount\n      rollingRatingPercentage\n      __typename\n    }\n    __typename\n  }\n  csp {\n    _id\n    webLink {\n      href\n      __typename\n    }\n    __typename\n  }\n  ...AddToCartButtonFields\n  ...ListingCardFields\n  ...ListingCreateOfferButtonData\n  ...InOtherCartsCardData\n  __typename\n}\n\nfragment AddToCartButtonFields on Listing {\n  _id\n  id\n  sellerId\n  listingType\n  pricing {\n    buyerPrice {\n      amount\n      amountCents\n      currency\n      __typename\n    }\n    __typename\n  }\n  preorderInfo {\n    onPreorder\n    estimatedShipDate {\n      seconds\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ListingCardSignalsData on reverb_signals_Signal {\n  name\n  title\n  icon\n  __typename\n}',
        }
        yield Request(url=self.start_urls[0], method="POST", headers=self.headers, body=json.dumps(json_data), callback=self.parse,)

    def parse(self, response, **kwargs):
        data = response.json()

        listings = data.get('data', {}).get('listingsSearch', {}).get('listings', [])
        total = data.get('data', {}).get('listingsSearch', {}).get('total', '')

        for item in listings[:2]:
            litem = OrderedDict()
            litem['Handle'] = item.get('slug', '').strip()
            litem['Title'] = item.get('title', '').strip()
            litem['Body (HTML)'] = ''
            litem['Vendor'] = item.get('shop', {}).get('name', '')
            litem['Type'] = (next((c.get('slug', '').replace('-', ' ') for c in item.get('categories', []) if c.get('slug')), ''))
            litem['Tag'] = ''
            litem['Published'] = self.is_published(item)
            litem['Option1 Name'] = 'Model'
            litem['Option1 Value'] = item.get('model', '')
            litem['Option2 Name'] = 'Make'
            litem['Option2 Value'] = item.get('make', '')
            litem['Option3 Name'] = ''
            litem['Option3 Value'] =''
            litem['Variant SKU'] = item.get('id', '')
            litem['Variant Grams'] = ''
            litem['Variant Inventory Tracker'] = ''
            litem['Variant Inventory Policy'] = 'deny' if (item.get('inventory') or 0) <= 0 else 'continue'
            litem['Variant Fulfillment Service'] = 'manual'
            litem['Variant Price'] = item.get('pricing', {}).get('buyerPrice', {}).get('display', '').replace('$', '')
            litem['Variant Compare At Price'] = litem.get('Variant Price', '')
            litem['Variant Requires Shipping'] = 'True' # its ensure physical product
            litem['Image Src'] = ''
            litem['Image Position'] = ''
            litem['Image Alt Text'] = ''
            litem['Gift Card'] = 'False'
            litem['SEO Title'] = litem.get('Title', '')
            litem['SEO Description'] = ''   
            litem['Google Shopping / Google Product Category'] = ''
            litem['Google Shopping (Unstructured metafield)'] = ''
            litem['Google Shopping / Age Group'] = ''
            litem['Google Shopping / MPN'] = ''
            litem['Google Shopping / AdWords Grouping'] = ''
            litem['Google Shopping / AdWords Labels'] = ''
            litem['Google Shopping / Condition'] = item.get('condition', {}).get('displayName', '')
            litem['Google Shopping / Custom Product'] = ''
            litem['Google Shopping / Custom Label 0'] = ''
            litem['Google Shopping / Custom Label 1'] = ''
            litem['Google Shopping / Custom Label 2'] = ''
            litem['Google Shopping / Custom Label 3'] = ''
            litem['Google Shopping / Custom Label 4'] = ''
            litem['Variant Image'] = ''
            litem['Variant Weight Unit'] = ''
            litem['Variant Tax Code'] = ''
            litem['Cost per item'] = ''
            litem['Status'] = {'sold': 'archived', 'active': 'active', 'draft': 'draft', 'archived': 'archived' }.get(
                                                            item.get('stateDescription', '').strip().lower(), 'active')
            litem['URL'] = f"{self.base_url}/item/{litem.get('Variant SKU', '')}-{item.get('slug', '')}?show_sold=true"

            proxy_url = f"https://proxy.scrapeops.io/v1/?api_key={self.api_key}&url={litem.get('URL', '')}"

            yield Request(url=proxy_url, headers=self.detail_headers, callback=self.parse_detail, meta={'litem': litem})

    def parse_detail(self, response):
        litem = response.meta.get('litem', {})
        ditem = {}
        # --- Core product data (same as before) ---
        ditem['Handle'] = litem.get('Handle', '').strip()
        ditem['Title'] = litem.get('Title', '').strip()
        ditem['Body (HTML)'] = ' '.join(response.css('div.item2-description__content').getall()).strip()
        ditem['Vendor'] = litem.get('Vendor', '')
        ditem['Type'] = litem.get('Type', '')
        ditem['Tags'] = ''
        ditem['Published'] = litem.get('Published', '')

        ditem.update({
            'Option1 Name': '', 'Option1 Value': '',
            'Option2 Name': '', 'Option2 Value': '',
            'Option3 Name': '', 'Option3 Value': '',
            'Variant SKU': litem.get('Variant SKU', ''),
            'Variant Inventory Tracker': '',
            'Variant Inventory Policy': litem.get('Variant Inventory Policy', ''),
            'Variant Fulfillment Service': 'manual',
            'Variant Requires Shipping': 'TRUE',
            'Gift Card': 'FALSE',
            'SEO Title': litem.get('Title', ''),
            'SEO Description': '',
            'Variant Tax Code': '',
            'Cost per item': '',
            'Status': litem.get('Status', ''),
            'URL': litem.get('URL', '')
        })

        # --- Weight ---
        body_html = ditem['Body (HTML)']
        weight_match = re.search(r'Weight:\s*([0-9\.]+\s*(?:kg|g|lb|lbs|oz))', body_html, re.IGNORECASE)
        product_weight = weight_match.group(1).strip() if weight_match else ""
        ditem['Variant Grams'] = self.in_gram(product_weight)
        units = re.findall(r'Weight:\s*[0-9\.]+\s*([a-zA-Z]+)', body_html, re.IGNORECASE)
        ditem['Variant Weight Unit'] = ' '.join(u.lower() for u in units) if units else ""

        price = litem.get('Variant Price', '')
        ditem['Variant Price'] = price
        ditem['Variant Compare At Price'] = price

        ditem['Google Shopping / Google Product Category'] = ' > '.join(
            response.css('ul.breadcrumbs span[itemprop="name"]::text').getall()
        )
        ditem['Google Shopping / Gender'] = ''
        ditem['Google Shopping / Age Group'] = ''
        ditem['Google Shopping / MPN'] = ''
        ditem['Google Shopping / AdWords Grouping'] = ''
        ditem['Google Shopping / AdWords Labels'] = ''
        ditem['Google Shopping / Condition'] = litem.get('Google Shopping / Condition', '')
        ditem['Google Shopping / Custom Product'] = ''
        for i in range(5):
            ditem[f'Google Shopping / Custom Label {i}'] = ''

        # --- Images ---
        image_srcs = self.clean_image_urls(response)
        image_alts = self.get_image_alts(response)
        image_positions = range(1, len(image_srcs) + 1)

        # --- 1️⃣ Write the full row first (with first image, if any) ---
        if image_srcs:
            ditem['Image Src'] = image_srcs[0]
            ditem['Image Position'] = 1
            ditem['Image Alt Text'] = image_alts[0] if image_alts else ''
            ditem['Variant Image'] = image_srcs[0]
        else:
            ditem['Image Src'] = ''
            ditem['Image Position'] = ''
            ditem['Image Alt Text'] = ''
            ditem['Variant Image'] = ''

        full_row = {col: ditem.get(col, '') for col in self.columns}
        self.write_to_csv(full_row)
        yield full_row

        # --- 2️⃣ Then repeat for remaining images (Handle, Image Src, Image Alt Text only) ---
        if len(image_srcs) > 1:
            for i in range(1, len(image_srcs)):
                repeat_row = {col: '' for col in self.columns}
                repeat_row['Handle'] = ditem['Handle']
                repeat_row['Image Src'] = image_srcs[i]
                repeat_row['Image Position'] = i + 1
                repeat_row['Image Alt Text'] = image_alts[i] if i < len(image_alts) else ''
                repeat_row['Variant Image'] = image_srcs[i]

                self.write_to_csv(repeat_row)
                yield repeat_row

    def get_image_alts(self, response):
        """Return list of alt texts matching product images."""
        return [alt.strip() if alt else "" for alt in
                response.css('div.lightbox-image__thumbs img::attr(alt)').getall()]

    def clean_image_urls(self, response):
        """Return full-size Shopify-safe image URLs."""
        urls = response.css('div.lightbox-image__thumbs img::attr(src)').getall()
        return [re.sub(r'(_thumb|_small|_medium)(\.[a-zA-Z]{3,4})(\?.*)?$', r'\2', response.urljoin(u)) for u in urls]

    def extract_images(self, response):
        srcs = self.clean_image_urls(response)
        alts = self.get_image_alts(response)
        positions = list(range(1, len(srcs) + 1))
        return list(zip_longest(srcs, positions, alts, fillvalue=""))

    def in_gram(self, product_weight):
        """Convert product weight text into grams."""
        if not product_weight:
            return None

        # Extract numeric and unit parts, e.g. "2.5 kg"
        m = re.match(r'([\d\.]+)\s*(kg|g|lb|lbs|oz)', product_weight.lower())
        if not m:
            return None

        n1, u1 = m.groups()
        grams = round(float(n1) * {'kg': 1000, 'g': 1, 'lb': 453.59237, 'lbs': 453.59237, 'oz': 28.34952}.get(u1, 0), 2)
        return grams

    def is_published(self, item):
        ts = item.get('publishedAt',{}).get('seconds','')
        if not ts:
            return True  # default published if missing or empty
        return datetime.utcfromtimestamp(ts) <= datetime.utcnow()

    def download_images(self, item):
        handle = str(item.get('Handle', 'Unknown')).replace('/', '-').replace(' ', '_')
        pid = str(item.get('Variant SKU', '0000'))
        folder_name = os.path.join("images", f"{handle}-{pid}")

        # Create folder only once (won’t fail if already exists)
        os.makedirs(folder_name, exist_ok=True)

        img_url = item.get('Image Src')
        if not img_url or not isinstance(img_url, str):
            self.logger.warning(f"⚠️ No valid image URL found for item {pid}")
            return

        try:
            response = requests.get(img_url.strip(), timeout=10)
            if response.status_code == 200:
                # Use proper extension or fallback to .jpg
                file_ext = os.path.splitext(img_url.split('?')[0])[1] or ".jpg"

                # Generate unique image filename if multiple images exist for same item
                existing_files = os.listdir(folder_name)
                image_count = sum(1 for f in existing_files if f.startswith("image"))
                filename = os.path.join(folder_name, f"image_{image_count + 1}{file_ext}")

                with open(filename, 'wb') as f:
                    f.write(response.content)

                self.logger.info(f"✅ Saved: {filename}")
                return filename  # optional
            else:
                self.logger.warning(f"⚠️ Failed to download {img_url} (status {response.status_code})")

        except Exception as e:
            self.logger.error(f"❌ Error downloading {img_url}: {e}")

    def write_to_csv(self, data: dict) -> None:
        """Write a single record to CSV with consistent headers and efficient handling."""
        file_exists = os.path.exists(self.output_file_name)

        with open(self.output_file_name, 'a', newline='', encoding='utf-8', buffering=1) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.columns, extrasaction='ignore')

            # Write header only if file is empty or newly created
            if not file_exists or os.path.getsize(self.output_file_name) == 0:
                writer.writeheader()

            # Ensure consistent field order and fill missing fields with empty strings
            row = {col: data.get(col, '') for col in self.columns}
            writer.writerow(row)

    # def download_images(self, item):
    #     handle = item.get('Handle', 'Unknown').replace('/', '-').replace(' ', '_')
    #     pid = item.get('Id', '0000')
    #     folder_name = f"images/{handle}-{pid}"
    #
    #     os.makedirs(folder_name, exist_ok=True)
    #
    #     for i, img_url in enumerate(item.get('Image Src', []), start=1):
    #         try:
    #             # Normalize URL
    #             #img_url = img_url if img_url.startswith('http') else self.base_url + img_url
    #
    #             response = requests.get(item.get('Image Src', []), timeout=10)
    #             if response.status_code == 200:
    #                 file_ext = os.path.splitext(img_url)[1].split('?')[0] or ".jpg"
    #                 filename = os.path.join(folder_name, f"image_{i}{file_ext}")
    #
    #                 with open(filename, 'wb') as f:
    #                     f.write(response.content)
    #
    #                 self.logger.info(f"✅ Saved: {filename}")
    #             else:
    #                 self.logger.warning(f"⚠️ Failed to download {img_url}")
    #         except Exception as e:
    #             self.logger.error(f"Error downloading {img_url}: {e}")

    
