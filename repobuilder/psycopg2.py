'''
This file is to avoid "ModuleNotFoundError: No module named 'psycopg2".
Some modules import psycopg2 but we actually don't use it.
It is enough that `psycopg2` is in import paths.
'''