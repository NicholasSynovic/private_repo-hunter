import argparse
from pathlib import Path

from pandas import DataFrame

from rh.datasets.joss import extract, load, transform
from rh.db import DB


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=("joss",), required=True)
    parser.add_argument("--output", type=Path, default=Path("rh.sqlite3"))
    args = parser.parse_args()

    db_path: Path = args.output.absolute()
    db: DB = DB(db_path=db_path)

    issues: list[dict] = extract()
    transformed_issues: DataFrame = transform(issues=issues)
    load(db=db, df=transformed_issues)


if __name__ == "__main__":
    main()
