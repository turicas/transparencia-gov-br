from scrapy import spiderloader
from scrapy.utils import project

settings = project.get_project_settings()
spider_loader = spiderloader.SpiderLoader.from_settings(settings)
spiders = spider_loader.list()
for spider_name in spiders:
    print(spider_name)
