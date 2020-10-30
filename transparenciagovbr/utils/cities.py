import rows

from transparenciagovbr import settings

cities_filename = settings.REPOSITORY_PATH / "data" / "populacao-estimada-2020.csv"
city_name_by_id = {
    row.city_ibge_code: row.city for row in rows.import_from_csv(cities_filename)
}
