import scrapy
import re
import logging
from imot_bg.items import ImotItem
from datetime import datetime
from scrapy_playwright.page import PageMethod
import asyncio

logger = logging.getLogger(__name__)

class ImotBgSpider(scrapy.Spider):
    name = 'imot_debug'
    allowed_domains = ['imot.bg', 'www.imot.bg']

    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'DOWNLOAD_DELAY': 2,
        'RETRY_TIMES': 2,
        'AUTOTHROTTLE_ENABLED': True,
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 60000,
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept-Language": "bg-BG,bg;q=0.9,en;q=0.8",
        "Referer": "https://www.imot.bg/"
    }

    def __init__(self, city='София', district='', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.city = city
        self.district = district.strip().lower()
        if not self.district:
            raise ValueError("❌ Обязательный аргумент 'district' не указан")
        logger.info(f"🛠️ Паук инициализирован для города: {self.city}, район: {self.district}")

    def start_requests(self):
        url = f'https://www.imot.bg/obiavi/prodazhbi/grad-sofiya/{self.district}'
        logger.info(f"🚀 Начинаю парсинг с URL: {url}")

        yield scrapy.Request(
            url=url,
            callback=self.parse_search_results,
            meta={
                'playwright': True,
                'playwright_include_page': True,
                'playwright_page_methods': [
                    PageMethod("wait_for_selector", "div.item", timeout=60000)
                ],
                'page': 1,
                'district': self.district
            },
            headers=self.headers,
            errback=self.parse_error
        )

    def parse_search_results(self, response):
        current_page = response.meta.get('page', 1)
        district = response.meta.get('district', 'unknown')

        logger.info(f"📄 Парсинг страницы {current_page} | URL: {response.url}")

        page = response.meta.get("playwright_page")
        if page:
            asyncio.create_task(page.close())

        if "captcha" in response.text.lower():
            logger.error("Обнаружена капча! Пропускаю страницу")
            return

        listings = response.css('div.item')
        if not listings:
            logger.warning(f"⚠️ На странице {current_page} нет объявлений")
            return

        for item in listings:
            try:
                relative_url = item.css('a::attr(href)').get()
                full_url = response.urljoin(relative_url) if relative_url else None

                if not full_url:
                    logger.warning("⚠️ Пропущено объявление без URL")
                    continue

                # сразу идём в parse_listing
                yield response.follow(
                    full_url,
                    callback=self.parse_listing,
                    meta={'district': district, 'page': current_page},
                    headers=self.headers,
                    errback=self.parse_error
                )
            except Exception as e:
                logger.error(f"❌ Ошибка парсинга ссылки на объявление: {str(e)}")

        # пагинация
        next_page = response.css('a.next::attr(href)').get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse_search_results,
                meta={
                    'playwright': True,
                    'playwright_include_page': True,
                    'playwright_page_methods': [
                        PageMethod("wait_for_selector", "div.item", timeout=60000)
                    ],
                    'page': current_page + 1,
                    'district': district
                },
                headers=self.headers,
                errback=self.parse_error
            )

    def parse_listing(self, response):
        logger.info(f"🏠 Обрабатываю страницу объявления: {response.url}")

        item = ImotItem()
        description = self.extract_description(response)

        rooms_text = response.xpath("//div[contains(text(), 'Тип имот')]/strong/text()").get() or ''
        item_rooms = self.determine_room_count(rooms_text)

        item.update({
            'source': 'imot.bg',
            'source_id': self.extract_id_from_url(response.url),
            'title': self.clean(response.css("div.advHeader div.title::text").get()),
            'currency': 'EUR',
            'price': self.clean_price(response.xpath("//div[@id='cena']/text()").get()),
            'price_sqm': self.clean_price(response.xpath("//span[@id='cenakv']/text()").get()),
            'area': self.clean_area(response.xpath("//div[contains(text(), 'Площ')]/strong/text()").get()),
            'rooms': item_rooms,
            'floor': self.clean(response.xpath("//div[contains(text(), 'Етаж')]/strong/text()").get()),
            'construction_type': self.clean(
                response.xpath("//div[contains(text(), 'Строителство')]/strong[1]/text()").get()),
            'year_built': self.extract_year(
                response.xpath("//div[contains(text(), 'Строителство')]/strong[2]/text()").get()),
            'description': description,
            'location': self.clean(response.css("div.location::text").get()),
            'district': response.meta.get('district'),
            'city': self.city,
            'agency': self.clean(response.css("div.name::text").get()),
            'phone': self.clean(response.css("div.phone::text").get()),
            'page_found': response.meta.get('page', 1),
            'url': response.url,
        })

        logger.info(f"✅ Сохранён item: {item.get('title')} — {item.get('price')} EUR")
        yield item

    def parse_error(self, failure):
        logger.error(f"🔥 Ошибка при обработке запроса: {failure.value}")

    @staticmethod
    def extract_description(response):
        description_html = response.xpath("//div[@id='description_div']").get()
        if description_html:
            text = re.sub(r'<br\s*/?>', '\n', description_html)
            return re.sub(r'<[^>]+>', '', text).strip()
        return None

    @staticmethod
    def determine_room_count(text):
        room_map = {
            "едностаен": 1, "1-стаен": 1,
            "двустаен": 2, "2-стаен": 2,
            "тристаен": 3, "3-стаен": 3,
            "многостаен": 4
        }
        return next((v for k, v in room_map.items() if k in text.lower()), None)

    @staticmethod
    def clean(text):
        return text.strip() if text else None

    @staticmethod
    def extract_id_from_url(url):
        match = re.search(r'obiava-([\w]+)', url)
        return match.group(1) if match else None

    @staticmethod
    def clean_price(text):
        if not text:
            return None
        try:
            num = re.sub(r'[^\d.]', '', text.replace(' ', '').replace(',', '.'))
            return float(num) if num else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def clean_area(text):
        if not text:
            return None
        try:
            num = re.search(r'([\d,.]+)', text)
            return float(num.group(1).replace(',', '.')) if num else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def extract_year(text):
        if not text:
            return None
        match = re.search(r'\d{4}', text)
        return int(match.group()) if match else None
