import importlib
import inspect
import logging
from pathlib import Path
from typing import Optional

import datajoint as dj
import numpy as np
from element_interface.utils import dict_to_uuid, find_full_path
from numpy.typing import DTypeLike

from .export.bossdb import BossDBUpload
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
        Mask: imaging segmentation mask for cell matching
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
        url: str,
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
        dtype: Optional[DTypeLike] = None,
        **kwargs,
    ):
        # NOTE: uploading from data_dir (local rel path) assumes 1 image per z slice
        # If not upload_from 'table', upload files in data_dir

        if upload_from == "table":
            data = (Volume & volume_key).fetch1("volume_data")
            dtype = dtype or data.dtype  # if provided, fetch from
        else:  # Uploading from image files
            data = None

            if not dtype:
                raise ValueError("Must specify dtype when loading data from images")

            if not data_dir and session_key:
                data_dir = find_full_path(
                    get_vol_root_data_dir(),
                    get_session_directory(session_key),
                )
            if not Path(data_dir).is_absolute():
                raise ValueError(f"Could not find absolute path to data: {data_dir}")

        if isinstance(dtype, str):
            dtype = np.dtype(dtype)
        if dtype and dtype not in [np.dtype("uint8"), np.dtype("uint16")]:
            raise ValueError("BossDB only accepts uint8 or uint16 image data.")

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

        bossdb = BossDBUpload(
            url=url,
            raw_data=data,
            data_dir=data_dir,
            voxel_size=(float(i) for i in (voxel_z_size, voxel_y_size, voxel_x_size)),
            voxel_units=voxel_unit,
            shape_zyx=(int(i) for i in (z_size, y_size, x_size)),
            resolution=downsampling,
            dtype=dtype,
            **kwargs,
        )
        bossdb.upload()


@schema
class SegmentationParamset(dj.Lookup):
    definition = """
    paramset_idx: int
    ---
    segmentation_method: varchar(32)
    paramset_desc="": varchar(256)
    params: longblob
    paramset_hash: uuid
    unique index (paramset_hash)
    """

    @classmethod
    def insert_new_params(
        cls,
        segmentation_method: str,
        paramset_desc: str = "",
        params: dict = {},
        paramset_idx: int = None,
    ):
        """Inserts new parameters into the table.

        Args:
            segmentation_method (str): name of the clustering method.
            paramset_desc (str): description of the parameter set
            params (dict): clustering parameters
            paramset_idx (int, optional): Unique parameter set ID. Defaults to None.
        """
        if paramset_idx is None:
            paramset_idx = (
                dj.U().aggr(cls, n="max(paramset_idx)").fetch1("n") or 0
            ) + 1

        param_dict = {
            "segmentation_method": segmentation_method,
            "paramset_desc": paramset_desc,
            "params": params,
            "paramset_idx": paramset_idx,
            "paramset_hash": dict_to_uuid(
                {**params, "segmentation_method": segmentation_method}
            ),
        }
        param_query = cls & {"paramset_hash": param_dict["paramset_hash"]}

        if param_query:  # If the specified param-set already exists
            existing_paramset_idx = param_query.fetch1("paramset_idx")
            if (
                existing_paramset_idx == paramset_idx
            ):  # If the existing set has the same paramset_idx: job done
                return
            else:  # If not same name: human error, trying to add the same paramset with different name
                raise dj.DataJointError(
                    f"The specified param-set already exists"
                    f" - with paramset_idx: {existing_paramset_idx}"
                )
        else:
            if {"paramset_idx": paramset_idx} in cls.proj():
                raise dj.DataJointError(
                    f"The specified paramset_idx {paramset_idx} already exists,"
                    f" please pick a different one."
                )
            cls.insert1(param_dict)


@schema
class SegmentationTask(dj.Manual):
    definition = """
    -> Volume
    ---
    task_mode='load': enum('load', 'trigger')
    -> [nullable] SegmentationParamset
    -> [nullable] URLs.Segmentation
    """


@schema
class Segmentation(dj.Imported):
    definition = """
    -> SegmentationTask
    ---
    segmentation_data=null: longblob
    """

    class Cell(dj.Part):
        definition = """
        -> master
        cell_id : int
        """

    def make(self, key):
        # NOTE: convert seg data to unit8 instead of uint64
        (task_mode, seg_method, resolution_id, url, params) = (
            SegmentationTask * SegmentationParamset * Resolution & key
        ).fetch1(
            "task_mode",
            "segmentation_method",
            "downsampling",
            "url",
            "params",
        )
        if task_mode == "trigger" or seg_method.lower() != "bossdb":
            raise NotImplementedError
        else:
            self.download(url=url, downsampling=resolution_id, **params)

    @classmethod
    def download(
        cls,
        url: Optional[str],
        downsampling: Optional[int] = 0,
        session_key: Optional[dict] = None,
        **kwargs,
    ):
        data = BossDBInterface(url, resolution=downsampling, session_key=session_key)
        data.load_data_into_element(table="Segmentation", **kwargs)


@schema
class CellMapping(dj.Computed):  # TODO: FIX cell table foreign key ref
    definition = """
    -> Segmentation.Cell
    -> Mask
    """

    def make(self, key):
        raise NotImplementedError


@schema
class ConnectomeParamset(dj.Lookup):
    definition = """
    paramset_idx: int
    ---
    connectome_method: varchar(32)
    paramset_desc="": varchar(256)
    params: longblob
    paramset_hash: uuid
    unique index (paramset_hash)
    """

    @classmethod
    def insert_new_params(
        cls,
        connectome_method: str,
        paramset_desc: str,
        params: dict,
        paramset_idx: int = None,
    ):
        """Inserts new parameters into the table.

        Args:
            connectome_method (str): name of the clustering method.
            paramset_desc (str): description of the parameter set
            params (dict): clustering parameters
            paramset_idx (int, optional): Unique parameter set ID. Defaults to None.
        """
        if paramset_idx is None:
            paramset_idx = (
                dj.U().aggr(cls, n="max(paramset_idx)").fetch1("n") or 0
            ) + 1

        param_dict = {
            "connectome_method": connectome_method,
            "paramset_desc": paramset_desc,
            "params": params,
            "paramset_idx": paramset_idx,
            "paramset_hash": dict_to_uuid(
                {**params, "connectome_method": connectome_method}
            ),
        }
        param_query = cls & {"paramset_hash": param_dict["paramset_hash"]}

        if param_query:  # If the specified param-set already exists
            existing_paramset_idx = param_query.fetch1("paramset_idx")
            if (
                existing_paramset_idx == paramset_idx
            ):  # If the existing set has the same paramset_idx: job done
                return
            else:  # If not same name: human error, trying to add the same paramset with different name
                raise dj.DataJointError(
                    f"The specified param-set already exists"
                    f" - with paramset_idx: {existing_paramset_idx}"
                )
        else:
            if {"paramset_idx": paramset_idx} in cls.proj():
                raise dj.DataJointError(
                    f"The specified paramset_idx {paramset_idx} already exists,"
                    f" please pick a different one."
                )
            cls.insert1(param_dict)


@schema
class ConnectomeTask(dj.Manual):
    definition = """
    -> Segmentation
    -> ConnectomeParamset
    ---
    task_mode='load': enum('load', 'trigger')
    -> [nullable] URLs.Connectome
    """


@schema
class Connectome(dj.Imported):
    definition = """
    -> ConnectomeTask
    """

    class Connection(dj.Part):
        definition = """
        -> Segmentation.Cell.proj(pre_synaptic='cell_id')
        -> Segmentation.Cell.proj(post_synaptic='cell_id')
        ---
        connectivity_strength: float # TODO: rename based on existing standards
        """

    def make(self, key):
        raise NotImplementedError
