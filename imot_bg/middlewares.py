import logging
import os
import random
from scrapy import signals
from scrapy.http import Request
from scrapy.downloadermiddlewares.retry import get_retry_request

logger = logging.getLogger(__name__)


class ImotBgSpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        return None

    def process_spider_output(self, response, result, spider):
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        logger.error(f"❗ Spider Exception: {exception} for URL: {getattr(response, 'url', 'N/A')}")

    def process_start_requests(self, start_requests, spider):
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        logger.info(f"🕷️ Spider opened: {spider.name}")


class ImotBgDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request: Request, spider):
        default_headers = {
            'User-Agent': request.headers.get('User-Agent', spider.settings.get('USER_AGENT')),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'bg-BG,bg;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.google.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        for key, val in default_headers.items():
            request.headers.setdefault(key, val)

        # Куки
        if spider.settings.getbool('COOKIES_ENABLED', False):
            request.cookies = request.cookies or {}
        else:
            request.cookies = {}

        logger.debug(f"➡️ Запрос к: {request.url}")
        return None

    def process_response(self, request, response, spider):
        spider.logger.debug(f"👁️ MIDDLEWARE response for: {response.url}")
        logger.info(f"📥 Ответ {response.status} от {response.url}")

        # Повторяем попытки на коды ошибок 522, 523, 524, 525
        if response.status in {522, 523, 524, 525}:
            reason = f"HTTP {response.status}"
            retry_req = get_retry_request(request, reason, spider)
            if retry_req:
                logger.warning(f"🔁 Повтор запроса из-за статуса {response.status}: {request.url}")
                return retry_req

        # Логируем и сохраняем нестандартные ответы
        if response.status != 200:
            district = request.meta.get('district', 'unknown')
            page = request.meta.get('page', 'unknown')
            safe_url = request.url.replace("/", "_").replace(":", "")
            file_name = f"bad_response_{district}_{page}_{safe_url}.html"
            path = os.path.join(os.getcwd(), file_name)

            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(response.text)
                logger.warning(f"💾 Ответ сохранён в файл: {file_name}")
            except Exception as e:
                logger.error(f"❌ Не удалось сохранить ответ: {e}")

        return response

    def process_exception(self, request, exception, spider):
        logger.error(f"❌ Downloader Exception: {exception} for URL: {request.url}")

    def spider_opened(self, spider):
        logger.info(f"🔧 Downloader middleware активирован для: {spider.name}")


class RandomUserAgentMiddleware:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/16.4 Safari/605.1.15",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_3 like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    ]

    def process_request(self, request: Request, spider):
        user_agent = random.choice(self.USER_AGENTS)
        request.headers['User-Agent'] = user_agent
        logger.debug(f"🎭 User-Agent установлен: {user_agent}")
