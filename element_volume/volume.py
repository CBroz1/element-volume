import importlib
import inspect
import logging
from pathlib import Path
from typing import Optional

import datajoint as dj
from datajoint.errors import DataJointError
from element_interface.utils import find_full_path

from .readers import BossDBInterface

logger = logging.getLogger("datajoint")

schema = dj.Schema()
_linking_module = None


def activate(
    schema_name: str,
    *,
    create_schema: bool = True,
    create_tables: bool = True,
    linking_module: str = None,
):
    """Activate this schema

    Args:
        schema_name (str): schema name on the database server to activate the `lab` element
        create_schema (bool): when True (default), create schema in the database if it
                            does not yet exist.
        create_tables (bool): when True (default), create schema tables in the database
                             if they do not yet exist.
        linking_module (str): A string containing the module name or module containing
            the required dependencies to activate the schema.

    Dependencies:
    Tables:
        Session: A parent table to Volume
        URLs: A table with any of the following volume_url, segmentation_url,
            connectome_url
    Functions:
        get_vol_root_data_dir: Returns absolute path for root data director(y/ies) with
            all volumetric data, as a list of string(s).
        get_session_directory: When given a session key (dict), returns path to
            volumetric data for that session as a list of strings.
    """

    if isinstance(linking_module, str):
        linking_module = importlib.import_module(linking_module)
    assert inspect.ismodule(
        linking_module
    ), "The argument 'linking_module' must be a module's name or a module"

    global _linking_module
    _linking_module = linking_module

    schema.activate(
        schema_name,
        create_schema=create_schema,
        create_tables=create_tables,
        add_objects=_linking_module.__dict__,
    )


# -------------------------- Functions required by the Element -------------------------


def get_vol_root_data_dir() -> list:
    """Fetches absolute data path to ephys data directories.

    The absolute path here is used as a reference for all downstream relative paths used in DataJoint.

    Returns:
        A list of the absolute path(s) to ephys data directories.
    """
    root_directories = _linking_module.get_vol_root_data_dir()
    if isinstance(root_directories, (str, Path)):
        root_directories = [root_directories]

    return root_directories


def get_session_directory(session_key: dict) -> str:
    """Retrieve the session directory with volumetric data for the given session.

    Args:
        session_key (dict): A dictionary mapping subject to an entry in the subject
            table, and session identifier corresponding to a session in the database.

    Returns:
        A string for the path to the session directory.
    """
    return _linking_module.get_session_directory(session_key)


# --------------------------------------- Schema ---------------------------------------


@schema
class Resolution(dj.Lookup):
    definition = """ # Resolution of stored data
    resolution_id: varchar(32) # Shorthand for convention. For BossDB, integer value.
    ---
    voxel_unit: varchar(16) # e.g., nanometers
    voxel_z_size: float # size of one z dimension voxel in voxel_units
    voxel_y_size: float # size of one y dimension voxel in voxel_units
    voxel_x_size: float # size of one x dimension voxel in voxel_units
    """


@schema
class Volume(dj.Manual):
    definition = """ # Dataset of a contiguous volume
    volume_id : varchar(32) # shorthand for this volume
    -> Resolution
    ---
    -> [nullable] Session
    z_size: int # total number of voxels in z dimension
    y_size: int # total number of voxels in y dimension
    x_size: int # total number of voxels in x dimension
    slicing_dimension='z': enum('x','y','z') # perspective of slices
    channel: varchar(64) # data type or modality (e.g., EM, segmentation, etc.)
    -> [nullable] URLs.Volume
    volume_data = null: longblob
    """

    @classmethod
    def upload(
        cls,
        volume_key: dict,
        session_key: Optional[dict] = None,
        data_dir: Optional[str] = None,  # Local absolute path
        **kwargs,
    ):
        # upload_increment (int):  For best performance, use be a multiple of 16.
        #   With a lot of RAM, 64. If out-of-memory errors, decrease to 16. If issues
        #   persist, try 8 or 4.

        from .export.bossdb import bossdb_upload  # isort: skip

        if not data_dir:
            if not session_key:
                raise DataJointError(
                    "Please provide either data_dir or session key to upload"
                )
            data_dir = find_full_path(
                get_vol_root_data_dir(),
                get_session_directory(session_key),
            )

        (
            url,
            resolution_id,
            z_size,
            y_size,
            x_size,
            voxel_z_size,
            voxel_y_size,
            voxel_x_size,
            voxel_unit,
        ) = (Volume * Resolution & volume_key).fetch1(
            "url",
            "resolution_id",
            "z_size",
            "y_size",
            "x_size",
            "voxel_z_size",
            "voxel_y_size",
            "voxel_x_size",
            "voxel_unit",
        )
        if resolution_id.isnumeric() and resolution_id != 0:
            raise ValueError(
                f"Cannot upload lower resolution data: resolution_id={resolution_id}"
            )

        bossdb_upload(
            url=url,
            data_dir=data_dir,
            voxel_size=(voxel_z_size, voxel_y_size, voxel_x_size),
            voxel_units=voxel_unit,
            shape_zyx=(z_size, y_size, x_size),
            source_channel=volume_key["volume_id"],
            **kwargs,
        )

    @classmethod
    def get_bossdb_data(self, volume_key: dict):
        url, resolution = (Volume & volume_key).fetch1("url", "resolution_id")
        return BossDBInterface(url, resolution=resolution)


class SegmentationParamset(dj.Params):
    definition = """
    id: int
    ---
    params: longblob
    segmentation_method: varchar(32)
    """


class SegmentationTask(dj.Manual):
    definition = """
    -> Volume
    -> SegmentationParamset
    ---
    task_mode='load': enum('load', 'trigger')
    -> [nullable] URLs.Segmentation
    """


class Segmentation(dj.Imported):
    defintion = """
    -> SegmentationTask
    """

    class Cell(dj.Part):
        definition = """
        call_id
        """

    def make(self, key):
        # NOTE: convert seg data to unit8 instead of uint64
        raise NotImplementedError


class CellMapping(dj.Computed):
    definition = """
    -> Segmentation.Cell
    ---
    -> imaging.Segmentation.Mask
    """

    def make(self, key):
        raise NotImplementedError


class ConnectomeParamset(dj.Params):
    definition = """
    id: int
    ---
    params: longblob
    segmentation_method: varchar(32)
    """


class ConnectomeTask(dj.Manual):
    defintion = """
    -> Segmentation
    -> ConnectomeParamset
    ---
    task_mode='load': enum('load', 'trigger')
    -> [nullable] URLs.Connectome
    """


class Connectome(dj.Imported):
    definition = """
    -> ConnectomeTask
    """

    class Connection(dj.Part):
        definition = """
        ---
        -> Cell.proj(pre_synaptic='cell_id')
        -> Cell.proj(post_synaptic='cell_id')
        connectivity_strength: float # TODO: rename based on existing standards
        """

    def make(self, key):
        raise NotImplementedError
