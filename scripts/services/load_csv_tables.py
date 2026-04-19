import psycopg2 as psycopg
import os

def load_images_bbox(**pg_params):
    with psycopg.connect(pg_params) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            pass