"""
Provides the base class that our models inherit, and sets the logging features for the entire app
"""

from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    text,
    Date,
    select,
    Boolean,
    create_engine,
    DDL,
    event
)
from sqlalchemy.orm import relationship, synonym
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property
import logging

Base = declarative_base()
metadata = Base.metadata


def prettyprinter(self, p, cycle):
    return p.text(self.__repr__())

Base._repr_pretty_ = prettyprinter


#logging
sql_log = logging.getLogger("sqlalchemy.engine")
sql_handler = logging.FileHandler("logs/SQL_Log.log", encoding="utf-8")
formatter = logging.Formatter(u"%(asctime)s: %(message)s", datefmt="%d-%m-%Y %H:%M")
sql_handler.setFormatter(formatter)
sql_log.addHandler(sql_handler)
sql_log.setLevel(logging.INFO)
sql_log.info("log running.")
