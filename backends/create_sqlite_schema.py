"""
Module to create catalog schema on a SQLite database. The module checks that the schema is valid, that the catalog does
not exist as well as a few other additional checks.
"""

import argparse
import sqlite3


def create_catalog_with_schema(catalog_path, schema_file):
    conn = sqlite3.connect(catalog_path)
    c = conn.cursor()
    # TODO...


if __name__ == "__main__":

    # Argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--schema-file', help='Schema definition file')
    parser.add_argument('--catalog-path', default="catserv.db", help='The catalog name')

    args = parser.parse_args()


