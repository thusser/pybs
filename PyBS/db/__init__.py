from contextlib import contextmanager
from sqlalchemy import MetaData, create_engine, event
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.orm import sessionmaker

from .base import Base
from .job import Job


class Database(object):
    """Manages the database connection for PyBS."""

    def __init__(self, connect: str):
        """Creates a new Database object.

        Examples for connect URI:
            * MySQL:
              mysql://pybs:pybs@localhost:3306/pybs
            * sqlite:
              sqlite:///home/pybs/pybs.db

        More examples at https://docs.sqlalchemy.org/en/latest/core/engines.html#

        Args:
            connect: URI for database connection.
        """

        # create engine
        self._engine = create_engine(connect)
        self._engine.echo = False
        event.listen(self._engine, 'checkout', Database._checkout_listener)

        # and metadata
        MetaData(self._engine)

        # and session
        self._session = sessionmaker(bind=self._engine)

        # create tables
        Base.metadata.create_all(self._engine, checkfirst=True)

    @staticmethod
    def _checkout_listener(dbapi_con, con_record, con_proxy):
        """Prevent MySQL timeouts.

        Taken from:
        https://stackoverflow.com/questions/18054224/python-sqlalchemy-mysql-server-has-gone-away
        """
        try:
            try:
                dbapi_con.ping(False)
            except TypeError:
                dbapi_con.ping()
            except AttributeError:
                # db doesn't seem to support pings...
                pass
        except dbapi_con.OperationalError as exc:
            if exc.args[0] in (2006, 2013, 2014, 2045, 2055):
                raise DisconnectionError()
            else:
                raise

    @contextmanager
    def __call__(self):
        """Provide a transactional scope around a series of operations."""
        session = self._session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


__all__ = ['Database', 'Job']
