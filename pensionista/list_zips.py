import json
from urllib.parse import urljoin
from urllib.request import urlopen


def ckan_package_resources(base_url, resource_id):
    template_url = urljoin(base_url, "/api/3/action/package_show?id={resource_id}")
    url = template_url.format(resource_id=resource_id)
    response = urlopen(url)
    data = json.loads(response.read())
    return data["result"]["resources"]


if __name__ == "__main__":
    resources = ckan_package_resources(
        base_url="http://www.dados.gov.br", resource_id="c76a1bc6-2330-4b05-b3dd-491124931496"
    )

    for resource in resources:
        if not resource["url"].lower().endswith(".zip"):
            continue
        print(resource["url"])
