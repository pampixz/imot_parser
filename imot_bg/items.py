import scrapy

class ImotItem(scrapy.Item):
    source = scrapy.Field()             # Источник данных (например, 'imot.bg')
    source_id = scrapy.Field()          # Уникальный ID объявления на сайте
    title = scrapy.Field()              # Заголовок объявления
    price = scrapy.Field()              # Цена объекта
    currency = scrapy.Field()           # Валюта цены (например, 'EUR', 'BGN')
    price_sqm = scrapy.Field()          # Цена за квадратный метр
    area = scrapy.Field()               # Площадь объекта (кв.м)
    rooms = scrapy.Field()              # Количество комнат
    floor = scrapy.Field()              # Этаж
    construction_type = scrapy.Field()  # Тип строения (например, тухла, панел)
    year_built = scrapy.Field()         # Год постройки
    condition = scrapy.Field()          # Състояние на имота (например, ново строителство)
    features = scrapy.Field()           # Характеристики / екстри (списък)
    description = scrapy.Field()        # Описание на имота
    location = scrapy.Field()           # Местоположение (адрес или квартал)
    district = scrapy.Field()           # Район
    city = scrapy.Field()               # Град
    url = scrapy.Field()                # URL към обявата
    images = scrapy.Field()             # Списък с URL-та към снимки
    agency = scrapy.Field()             # Агенция или лице, публикувало обявата
    phone = scrapy.Field()              # Телефон за контакт
    scraped_at = scrapy.Field()         # Време на събиране на данните
    page_found = scrapy.Field()
