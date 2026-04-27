from pathlib import Path

from pandas import DataFrame

from rh.datasets.joss import extract, load, transform
from rh.db import DB


def main():
    db_path: Path = Path("rh.sqlite3").absolute()
    db: DB = DB(db_path=db_path)

    issues: list[dict] = extract()
    transformed_issues: DataFrame = transform(issues=issues)
    load(db=db, df=transformed_issues)


if __name__ == "__main__":
    main()
