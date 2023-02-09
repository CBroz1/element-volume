import importlib
import inspect
import logging
from pathlib import Path
from typing import Optional

import datajoint as dj
from element_interface.utils import find_full_path

from .export.bossdb import bossdb_upload
from .readers.bossdb import BossDBInterface

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
    resolution_id: varchar(32) # Shorthand for convention
    ---
    voxel_unit: varchar(16) # e.g., nanometers
    voxel_z_size: float # size of one z dimension voxel in voxel_units
    voxel_y_size: float # size of one y dimension voxel in voxel_units
    voxel_x_size: float # size of one x dimension voxel in voxel_units
    downsampling=0: int # Downsampling iterations relative to raw data
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
    channel: varchar(64) # data type or modality
    -> [nullable] URLs.Volume
    volume_data = null: longblob # Upload assumes (Z, Y, X) np.array
    """

    @classmethod
    def download(
        cls,
        url: Optional[str],
        downsampling: Optional[int] = 0,
        session_key: Optional[dict] = None,
        **kwargs,
    ):
        data = BossDBInterface(url, resolution=downsampling, session_key=session_key)
        data.insert_channel_as_url(data_channel="Volume")
        data.load_data_into_element(**kwargs)

    @classmethod
    def return_bossdb_data(self, volume_key: dict):
        url, res_id = (Volume & volume_key).fetch1("url", "resolution_id")
        downsampling = (Resolution & dict(resolution_id=res_id)).fetch("downsampling")
        return BossDBInterface(url, resolution=downsampling)

    @classmethod
    def upload(
        cls,
        volume_key: dict,
        session_key: Optional[dict] = None,
        upload_from: Optional[str] = "table",
        data_dir: Optional[str] = None,
        **kwargs,
    ):
        # NOTE: uploading from data_dir (local rel path) assumes 1 image per z slice
        # If not upload_from 'table', upload files in data_dir

        if not data_dir and session_key:
            data_dir = find_full_path(
                get_vol_root_data_dir(),
                get_session_directory(session_key),
            )

        (
            url,
            downsampling,
            z_size,
            y_size,
            x_size,
            voxel_z_size,
            voxel_y_size,
            voxel_x_size,
            voxel_unit,
        ) = (Volume * Resolution & volume_key).fetch1(
            "url",
            "downsampling",
            "z_size",
            "y_size",
            "x_size",
            "voxel_z_size",
            "voxel_y_size",
            "voxel_x_size",
            "voxel_unit",
        )

        if upload_from == "table":
            data = (Volume & volume_key).fetch1("volume_data")
        else:
            data = None

        bossdb_upload(
            url=url,
            raw_data=data,
            data_dir=data_dir,
            voxel_size=(voxel_z_size, voxel_y_size, voxel_x_size),
            voxel_units=voxel_unit,
            shape_zyx=(z_size, y_size, x_size),
            resolution=downsampling,
            source_channel=volume_key["volume_id"],
            **kwargs,
        )


class SegmentationParamset(dj.Lookup):
    definition = """
    id: int
    ---
    params: longblob
    segmentation_method: varchar(32)
    unique index (params)
    """


class SegmentationTask(dj.Manual):
    definition = """
    -> Volume
    ---
    task_mode='load': enum('load', 'trigger')
    -> [nullable] SegmentationParamset
    -> [nullable] URLs.Segmentation
    """


class Segmentation(dj.Imported):
    defintion = """
    -> SegmentationTask
    ---
    segmentation_data=null: longblob
    """

    class Cell(dj.Part):
        definition = """
        call_id
        """

    def make(self, key):
        # NOTE: convert seg data to unit8 instead of uint64
        task_mode = (SegmentationTask & key).fetch1("task_mode")
        if task_mode == "trigger":
            raise NotImplementedError
        else:
            (SegmentationTask * Volume & key).fetch("experiment_")

    @classmethod
    def download(
        cls,
        url: Optional[str],
        downsampling: Optional[int] = 0,
        session_key: Optional[dict] = None,
        **kwargs,
    ):
        data = BossDBInterface(url, resolution=downsampling, session_key=session_key)
        data.insert_channel_as_url(data_channel="Volume")
        data.load_data_into_element(**kwargs)


class CellMapping(dj.Computed):
    definition = """
    -> Segmentation.Cell
    ---
    -> imaging.Segmentation.Mask
    """

    def make(self, key):
        raise NotImplementedError


class ConnectomeParamset(dj.Lookup):
    definition = """
    id: int
    ---
    params: longblob
    connectome_method: varchar(32)
    unique index (params)
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
