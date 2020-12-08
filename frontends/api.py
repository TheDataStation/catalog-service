import argparse
from backends.backend_api import SQLiteBackend
from backends.catserv_api import CatalogService

def create_backend(catalog_type="sqlite",catalog_path="catserv.db"):
    if catalog_type == "sqlite":
        print("create sqlite backend at" + catalog_path)
        return SQLiteBackend(catalog_path)

def create_api(catalog_type, catalog_path):
    # create backend
    print("create_api " + catalog_type+" "+catalog_path)
    catalog = create_backend(catalog_type, catalog_path)
    # compose backend into main catalog service API
    return CatalogService(catalog)


if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend-type', default='sqlite', help='The backend type (default is SQLite')
    parser.add_argument('--catalog-path', help='The catalog name')

    args = parser.parse_args()
    # TODO...
