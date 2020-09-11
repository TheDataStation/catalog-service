import argparse
from backends import backend_api as bk
from catserv_api import CatalogService


def create_api(catalog_type, catalog_path):
    # create backend
    catalog = bk.create_catalog_type(catalog_type, catalog_path)
    # compose backend into main catalog service API
    return CatalogService(catalog)


if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend-type', default='sqlite', help='The backend type (default is SQLite')
    parser.add_argument('--catalog-path', help='The catalog name')

    args = parser.parse_args()
    # TODO...
