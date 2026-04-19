import dotenv
import os
from minio import Minio
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker

dotenv.load_dotenv()
MINIO_USER = os.getenv("MINIO_USER", "admin")
MINIO_PASS = os.getenv("MINIO_PASSWORD", "password")
MINIO_PORT = os.getenv("MINIO_API_PORT", "9000")
MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "password")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_DB = os.getenv("PG_DB", "postgres")

minio_client = Minio(
    endpoint=MINIO_HOST + ":" + MINIO_PORT,
    access_key=MINIO_USER,
    secret_key=MINIO_PASS,
    secure=False
)

pg_link = URL.create(
    "postgresql+psycopg2",
    username=PG_USER,
    password=PG_PASS,
    host=PG_HOST,
    port=int(PG_PORT),
    database=PG_DB,
)

pg_engine = create_engine(pg_link, pool_size=20, max_overflow=20, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=pg_engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_minio_client() -> Minio:
    return minio_client

if __name__ == '__main__':
    pass
