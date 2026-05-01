import json
import logging
import re

from fastcore.all import AttrDict, obj2dict
from pandas import DataFrame, Timestamp
from pydantic import BaseModel

from rh.apis.github import GitHub
from rh.db import DB
from rh.logger import get_logger, log_method

GITHUB_AUTHOR: str = "openjournals"
GITHUB_REPO: str = "joss-reviews"

LOGGER: logging.Logger = get_logger(__name__)


class Issue(BaseModel):
    issue_id: int
    created_at: int
    closed_at: int
    labels: str
    accepted: bool = False
    repository_url: str = ""
    paper_url: str = ""
    raw_json: str

    @log_method
    @staticmethod
    def convert_issue_id(issue_id) -> int:
        return int(issue_id)

    @log_method
    @staticmethod
    def convert_datetime(dt: str) -> int:
        try:
            return int(Timestamp(ts_input=dt).timestamp())
        except TypeError:
            return -1

    @log_method
    @staticmethod
    def convert_labels(labels: list[AttrDict]) -> str:
        data: list[str] = []

        label: AttrDict
        for label in labels:
            label_dict: dict = obj2dict(label)
            data.append(label_dict["name"])

        return json.dumps(obj=sorted(data))

    @log_method
    @staticmethod
    def convert_raw_json(raw_json: dict) -> str:
        return json.dumps(obj=obj2dict(raw_json), indent=4)

    @log_method
    def extract_accepted_label(self) -> None:
        self.accepted = '"accepted"' in self.labels

    @log_method
    def extract_repository_url(self, body: str) -> None:
        # Handle issues with no body content
        try:
            split_body: list[str] = body.splitlines()
        except AttributeError:
            return

        # Handle non-paper submission issues
        if len(split_body) < 13:
            return

        match = re.search(r'href="([^"]*)"', split_body[1])
        if match:
            self.repository_url = match.group(1)

    @log_method
    def extract_paper_url(self, body: str) -> None:
        if self.accepted is False:
            return None

        pattern = re.compile(r"\]\((https?://joss\.theoj\.org/papers/[^)]+)\)$")

        split_body: list[str] = body.splitlines()
        line: str
        for line in split_body[8:12]:
            match = pattern.search(line)
            if match:
                self.paper_url = match.group(1)


def extract() -> list[dict]:
    LOGGER.info("Extracting %s/%s issues...", GITHUB_AUTHOR, GITHUB_REPO)
    gh: GitHub = GitHub(owner=GITHUB_AUTHOR, repo=GITHUB_REPO)
    return gh.get_all_issues()


def transform(issues: list[dict]) -> DataFrame:
    data: list[Issue] = []

    LOGGER.info(
        "Transforming %s/%s issues... count=%s",
        GITHUB_AUTHOR,
        GITHUB_REPO,
        len(issues),
    )
    for issue in issues:
        datum: Issue = Issue(
            issue_id=Issue.convert_issue_id(issue["number"]),
            created_at=Issue.convert_datetime(issue["created_at"]),
            closed_at=Issue.convert_datetime(issue["closed_at"]),
            labels=Issue.convert_labels(issue["labels"]),
            raw_json=Issue.convert_raw_json(raw_json=issue),
        )
        datum.extract_accepted_label()
        datum.extract_repository_url(body=issue["body"])
        datum.extract_paper_url(body=issue["body"])

        data.append(datum)

    LOGGER.info(
        "Finished transforming %s/%s issues.",
        GITHUB_AUTHOR,
        GITHUB_REPO,
    )

    return DataFrame(data=[issue.model_dump() for issue in data])


def load(db: DB, df: DataFrame) -> None:
    df.to_sql(
        name="joss",
        con=db.engine,
        if_exists="append",
        index=True,
        index_label="id",
    )
