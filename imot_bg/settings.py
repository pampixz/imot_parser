import sys
import logging
from shutil import which

# Windows event loop fix
if sys.platform.startswith("win"):
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

BOT_NAME = "imot_bg"
SPIDER_MODULES = ["imot_bg.spiders"]
NEWSPIDER_MODULE = "imot_bg.spiders"

# Настройки Playwright
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "timeout": 60000,
    "args": [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",  # Важно для Docker/серверов
        "--single-process"  # Для стабильности
    ]
}
PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 4  # Оптимально для 8 CONCURRENT_REQUESTS
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60000

# Обработчики загрузки
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Основные настройки
ROBOTSTXT_OBEY = False
COOKIES_ENABLED = False  # Лучше отключить для playwright
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 2.5  # Увеличено для стабильности
DOWNLOAD_TIMEOUT = 90  # Увеличено для playwright

# AutoThrottle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 3
AUTOTHROTTLE_MAX_DELAY = 15
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.5  # Более консервативно

# Middlewares
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': None,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 100,
    # 'scrapy_playwright.middleware.PlaywrightMiddleware': 800,  # <- УДАЛИТЬ ЭТУ СТРОКУ
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    # 'scrapy_playwright.middleware.ScrapyPlaywrightDownloadHandler': 543, # <- И ЭТУ ТОЖЕ УДАЛИТЬ
}

# Retry policy
RETRY_TIMES = 5  # Увеличено для playwright
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429, 403, 404]
RETRY_PRIORITY_ADJUST = -1  # Для более быстрых повторных попыток

# Pipelines
ITEM_PIPELINES = {
    'imot_bg.pipelines.PostgresPipeline': 300,
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
# Логирование
LOG_LEVEL = 'INFO'
logging.getLogger('scrapy').setLevel(logging.WARNING)
logging.getLogger('scrapy-playwright').setLevel(logging.INFO)
logging.getLogger('twisted').setLevel(logging.ERROR)

# User-Agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Дополнительные настройки
FEED_EXPORT_ENCODING = "utf-8"
HTTPCACHE_ENABLED = False
AJAXCRAWL_ENABLED = True
REACTOR_THREADPOOL_MAXSIZE = 20  # Для async операций

