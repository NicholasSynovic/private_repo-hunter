import json
from typing import Any

from fastcore.all import AttrDict, L, obj2dict
from pandas import DataFrame, Timestamp
from progress.bar import Bar
from pydantic import BaseModel

from rh.apis.github import GitHub
from rh.db import DB

GITHUB_AUTHOR: str = "openjournals"
GITHUB_REPO: str = "joss-reviews"


class Issue(BaseModel):
    issue_id: int
    created_at: int
    closed_at: int
    labels: str
    raw_json: str

    @staticmethod
    def convert_issue_id(issue_id) -> int:
        return int(issue_id)

    @staticmethod
    def convert_datetime(dt: str) -> int:
        return int(Timestamp(ts_input=dt).timestamp())

    @staticmethod
    def convert_labels(labels: list[AttrDict]) -> str:
        data: list[str] = []
        label: AttrDict
        for label in labels:
            label_dict: dict = obj2dict(label)
            data.append(label_dict["name"])
        return json.dumps(obj=sorted(data))

    @staticmethod
    def convert_raw_json(raw_json: dict) -> str:
        return json.dumps(obj=obj2dict(raw_json), indent=4)


def extract() -> list[dict]:
    print(f"Extracting {GITHUB_AUTHOR}/{GITHUB_REPO} issues...")
    gh: GitHub = GitHub(owner=GITHUB_AUTHOR, repo=GITHUB_REPO)
    return gh.get_all_issues()


def transform(issues: list[dict]) -> DataFrame:
    data: list[Issue] = []

    with Bar(
        f"Transforming {GITHUB_AUTHOR}/{GITHUB_REPO} issues... ", max=len(issues)
    ) as bar:
        issue: dict[str, Any]
        for issue in issues:
            datum: Issue = Issue(
                issue_id=Issue.convert_issue_id(issue["number"]),
                created_at=Issue.convert_datetime(issue["created_at"]),
                closed_at=Issue.convert_datetime(issue["closed_at"]),
                labels=Issue.convert_labels(issue["labels"]),
                raw_json=Issue.convert_raw_json(raw_json=issue),
            )
            data.append(datum)
            bar.next()

    return DataFrame(data=[issue.model_dump() for issue in data])


def load(db: DB, df: DataFrame) -> None:
    df.to_sql(
        name="joss",
        con=db.engine,
        if_exists="append",
        index=True,
        index_label="id",
    )
