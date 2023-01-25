import logging

import datajoint as dj

logger = logging.getLogger("datajoint")

schema = dj.Schema()


def activate(schema_name: str, create_schema: bool = True, create_tables: bool = True):
    """Activate this schema

    Args:
        schema_name (str): schema name on the database server to activate the `lab` element
        create_schema (bool): when True (default), create schema in the database if it
                            does not yet exist.
        create_tables (bool): when True (default), create schema tables in the database
                             if they do not yet exist.
    """

    schema.activate(
        schema_name, create_schema=create_schema, create_tables=create_tables
    )


@schema
class Volume(dj.Manual):
    definition = """
    -> Session
    volume_id : int
    """

    class File(dj.Part):
        definition = """
        -> master
        file_path: varchar(255)  # filepath relative to root data directory
        """
