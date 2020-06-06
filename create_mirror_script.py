import os
import stat
from textwrap import dedent
from urllib.parse import urlparse

from scrapy import spiderloader
from scrapy.utils import project

from transparenciagovbr.utils.date import date_range, date_to_dict

output_filename = "mirror.sh"
settings = project.get_project_settings()
spider_loader = spiderloader.SpiderLoader.from_settings(settings)
spiders = spider_loader.list()
with open(output_filename, mode="w") as fobj:
    fobj.write(
        dedent(
            """
    #!/bin/bash

    mirror_file() {
        url="$1"
        download_path="$2"
        mirror_uri="$3"

        aria2c \\
                --summary-interval=0 \\
                --dir=$(dirname "$download_path") \\
                --out=$(basename "$download_path") \\
                "$url"
        if [ -e "$download_path" ]; then
            s3cmd put "$download_path" "$mirror_uri"
            rm "$download_path"
        fi
    }
    """
        ).strip()
    )
    fobj.write(f"\nmkdir -p {settings['DOWNLOAD_PATH']}\n")
    for spider_name in spiders:
        fobj.write(f"\n# {spider_name}\n")
        SpiderClass = spider_loader.load(spider_name)
        for date in date_range(
            start=SpiderClass.start_date,
            stop=SpiderClass.end_date,
            interval=SpiderClass.publish_frequency,
        ):
            url = SpiderClass.base_url.format(**date_to_dict(date))
            filename = urlparse(url).path.rsplit("/", maxsplit=1)[-1]
            mirror_uri = f"s3://mirror/transparenciagovbr/{spider_name}/{filename}"
            download_path = settings["DOWNLOAD_PATH"] / filename
            fobj.write(f"mirror_file {url} {download_path} {mirror_uri}\n")
# chmod 750 mirror.sh
os.chmod(
    output_filename,
    stat.S_IRUSR + stat.S_IWUSR + stat.S_IXUSR + stat.S_IRGRP + stat.S_IXGRP,
)
