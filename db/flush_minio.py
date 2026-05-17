import argparse
import os

from dotenv import load_dotenv

from model.minio_store import MinioStore

load_dotenv()


def run(bucket: str) -> None:
    store = MinioStore(bucket)
    n     = store.flush_all()
    print(f"Deleted {n} objects from '{store.bucket}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flush all objects from a MinIO bucket.")
    parser.add_argument(
        "bucket",
        nargs="?",
        default=os.getenv("MINIO_BUCKET", "market-data"),
        help="Bucket to flush (default: $MINIO_BUCKET or market-data)",
    )
    args = parser.parse_args()
    run(args.bucket)
