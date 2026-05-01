import argparse
import logging
from pathlib import Path

from pandas import DataFrame

from rh.datasets.joss import extract, load, transform
from rh.db import DB
from rh.logger import configure_logging


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=("joss",), required=True)
    parser.add_argument("--output", type=Path, default=Path("rh.sqlite3"))
    parser.add_argument("--log-path", type=Path, default=None)
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
    )
    args = parser.parse_args()

    configure_logging(log_path=args.log_path, level=getattr(logging, args.log_level))

    db_path: Path = args.output.absolute()
    db: DB = DB(db_path=db_path)

    issues: list[dict] = extract()
    transformed_issues: DataFrame = transform(issues=issues)
    load(db=db, df=transformed_issues)


if __name__ == "__main__":
    main()
