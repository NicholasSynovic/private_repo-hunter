from pathlib import Path

from sqlalchemy import (Boolean, Column, Engine, ForeignKey, Integer, MetaData,
                        String, Table, create_engine)


class DB:
    def __init__(self, db_path: Path) -> None:
        self._path: Path = db_path.absolute()

        self.engine: Engine = create_engine(url=f"sqlite:///{self._path}")
        self.metadata: MetaData = MetaData()

        self._create_tables()

    def _create_tables(self) -> None:
        _: Table = Table(
            "joss",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("issue_id", Boolean),
            Column("created_at", String),
            Column("closed_at", String),
            Column("labels", String),
            Column("raw_json", String),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)
