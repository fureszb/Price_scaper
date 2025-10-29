########################################
# Scrapy alap konfiguráció
########################################

BOT_NAME = "arkereso"

SPIDER_MODULES = ["arkereso.spiders"]
NEWSPIDER_MODULE = "arkereso.spiders"

ROBOTSTXT_OBEY = True

CONCURRENT_REQUESTS_PER_DOMAIN = 1


FEED_EXPORT_ENCODING = "utf-8"

########################################
# Playwright integráció (STABIL)
########################################

# Letöltő handler Playwright-hoz (MEGFELELŐ!!)
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Twisted reactor (kötelező Playwright-hoz)
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# Playwright használat kapcsoló (main.py felülírja)
PLAYWRIGHT_ENABLED = True  # vagy False (main.py-ban váltjuk)

# Browser típusa
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHTBROWSERTYPE = "chromium"


# Debug mód alapértéke (main.py felülírja)
PLAYWRIGHT_DEBUG = True  # True = látható böngésző, False = headless

# Böngésző indítási opciók
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": not PLAYWRIGHT_DEBUG,  # ha debug True, itt False lesz
    #"headless": True,
    "slow_mo": 300 if PLAYWRIGHT_DEBUG else 0,  # emberi tempó vizuális módnál
}

# Context beállítás (pl. viewport, user agent)
PLAYWRIGHT_CONTEXTS = {
    "default": {
        "viewport": {"width": 1280, "height": 720},
        "user_agent": None,  # később random User-Agent-et is tudunk adni
    }
}

########################################
# Middleware aktiválás
########################################
DOWNLOADER_MIDDLEWARES = {
    "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler": 543,
}



########################################
# Pipeline konfiguráció
########################################
#ITEM_PIPELINES = {
 #   "arkereso.pipelines.ExcelPipeline": 300,
#}

########################################
# Logger
########################################
LOG_ENABLED = True
LOG_LEVEL = "INFO"

########################################
# Egyéb beállítások
########################################
OUTPUT_FILE = "output.xlsx"

# Alap user agent lista
USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 10; SM-G970F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
]

# Timeout beállítás
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60000  # 60s

DEFAULT_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
}

DOWNLOAD_DELAY = 3
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 403]

ITEM_PIPELINES = {
    'arkereso.pipelines.AIValidationPipeline': 300,
    'arkereso.pipelines.ExcelPipeline': 800,
}
