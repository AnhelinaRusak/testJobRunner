"""Module for managing DASA Metrics Database"""
from typing import Any, Callable

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from credentials import get_credentials
from logger import log
from .job_table import JobTable


def _commit_changes(func: Callable) -> Callable:
    """Commit changes into Database. If changes raises Exception rollback Database to previous state"""
    def wrapper(self: JobTable, *args: Any, **kwargs: Any) -> Any:
        try:
            result = func(self, *args, **kwargs)
            self.session.commit()
            log.info('Successfully committed changes')
            return result
        except Exception as e:
            self.session.rollback()
            log.error('Rollback to previous session state')
            raise e

    return wrapper


class JobTableTools:
    """Base class for managing DASA Metrics Database"""

    def __init__(self) -> None:
        """Create DASAMetricsDBTools and initialize session"""
        credentials = get_credentials('DASAMetricsDB')
        host = credentials['host']
        user = credentials['user']
        password = credentials['password']
        database = credentials['database']
        self.engine = create_engine(f'postgresql://{user}:{password}@{host}/{database}')
        session = sessionmaker(bind=self.engine)
        self.session = session()

    @_commit_changes
    def create_record(self, record: JobTable) -> None:
        """Add record into Database"""
        self.session.add(record)

    @_commit_changes
    def commit_changes(self) -> None:
        """Commit changes into Database"""
        pass

    @_commit_changes
    def delete_record(self, record: JobTable) -> None:
        """Delete record from Database"""
        self.session.delete(record)

    @_commit_changes
    def get_record(self, uuid: str) -> JobTable | None:
        """Get record from Database by primary key. Returns None if record does not exist."""
        return self.session.query(JobTable).get(uuid)
