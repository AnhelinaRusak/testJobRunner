"""
Module for description of Job table structure.
In case of any changes in tables structure follow the steps, described in documentation:
https://dtt-it.atlassian.net/wiki/spaces/AIDA/pages/3006628184/Code+Management
"""
from sqlalchemy import Column, String, Integer, DateTime, func
from sqlalchemy.ext.declarative import declarative_base

BaseTable = declarative_base()


class JobTable(BaseTable):
    """Class for description of Job table structure"""

    __tablename__ = 'Jobs'

    uuid = Column(String, primary_key=True, name='UUID')
    type = Column(String, name='Type', nullable=False)
    status = Column(String, name='Status', nullable=False)
    retry = Column(Integer, name='Retry', default=0)
    container = Column(String, name='Container')
    machine = Column(String, name='Machine')
    created_on = Column(DateTime, name='CreatedOn', server_default=func.now())
    modified_on = Column(DateTime, name='ModifiedOn', server_default=func.now(), server_onupdate=func.now())
