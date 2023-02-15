"""
Optional schema to provide URLs to the main volume schema.
"""
import logging

import datajoint as dj

schema = dj.Schema()

logger = logging.getLogger("datajoint")


def activate(
    schema_name: str,
    *,
    create_schema: bool = True,
    create_tables: bool = True,
):
    """Activate this schema

    Args:
        schema_name (str): schema name on the database server to use for activation
        create_schema (bool): when True (default), create schema in the database if it
            does not yet exist.
        create_tables (bool): when True (default), create schema tables in the database
            if they do not yet exist.
    """

    schema.activate(
        schema_name,
        create_schema=create_schema,
        create_tables=create_tables,
    )


@schema
class BossDBURLs(dj.Lookup):
    definition = """
    collection_experiment : varchar(64)
    """

    class Volume(dj.Part):
        definition = """
        -> master
        url: varchar(64)
        """

    class Segmentation(dj.Part):
        definition = """
        -> master
        url: varchar(64)
        """

    class Connectome(dj.Part):
        definition = """
        url: varchar(64)
        """

    @classmethod
    def load_bossdb_info(
        cls,
        collection: str,
        experiment: str,
        volume: str,
        segmentation: str = "",
        connectome: str = "",
        skip_duplicates: bool = False,
        test_exists: bool = False,  # Run a check to see if the data already exists
    ):

        from .readers.bossdb import BossDBInterface  # isort: skip

        collection_experiment = f"{collection}/{experiment}"
        master_key = dict(collection_experiment=collection_experiment)
        base_url = f"bossdb://{collection_experiment}/"
        vol_url = base_url + volume
        seg_url = base_url + segmentation
        con_url = base_url + connectome

        if test_exists:
            for url in [vol_url, seg_url, con_url]:
                if url != base_url and not BossDBInterface(url).exists:
                    logger.warning(
                        f"The following BossDB url does not yet exist: {url}"
                    )

        with cls.connection.transaction:
            cls.insert1(master_key, skip_duplicates=skip_duplicates)

            cls.Volume.insert1(
                {**master_key, "url": vol_url},
                skip_duplicates=skip_duplicates,
            )
            if segmentation:
                cls.Segmentation.insert1(
                    {**master_key, "url": seg_url},
                    skip_duplicates=skip_duplicates,
                )
            if connectome:
                cls.Connectome.insert1(
                    {**master_key, "url": con_url},
                    skip_duplicates=skip_duplicates,
                )

    @classmethod
    def return_neuroglancer_url(cls, key, table="Volume"):
        url = (getattr(cls, table) & key).fetch1("url")

        return (
            "https://neuroglancer.bossdb.io/#!{'layers':{'image':{'source':'"
            + url
            + "'}}}"
        )
