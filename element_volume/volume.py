import importlib
import inspect
import logging
import pathlib

import datajoint as dj

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
    ), "The argument 'dependency' must be a module's name or a module"

    global _linking_module
    _linking_module = linking_module

    schema.activate(
        schema_name,
        create_schema=create_schema,
        create_tables=create_tables,
        add_objects=_linking_module.__dict__,
    )


# -------------- Functions required by the elements-ephys  ---------------


def get_vol_root_data_dir() -> list:
    """Fetches absolute data path to ephys data directories.

    The absolute path here is used as a reference for all downstream relative paths used in DataJoint.

    Returns:
        A list of the absolute path(s) to ephys data directories.
    """
    root_directories = _linking_module.get_vol_root_data_dir()
    if isinstance(root_directories, (str, pathlib.Path)):
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
class Zoom(dj.Lookup):
    definition = """ # Image cutoffs when taking a subset of a given slice
    zoom_id: varchar(32) # Shorthand for zoom convention
    ---
    first_start: int # Starting voxel in first dimension (X if taking Z slices)
    first_end=null: int # Ending voxel plus 1 in first dimension
    second_start: int # Starting voxel in second dimension (Y if taking Z slices)
    second_end=null: int # Ending voxel plus 1 in second dimension
    """

    contents = [
        ("Full Image", 0, None, 0, None),
    ]


@schema
class Volume(dj.Manual):
    # NOTE: Session added as nullable because data downloaded from BossDB would not be
    # associated with a session. Should we enforce this association?
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
    url=null : varchar(255) # dataset URL
    """

    class Slice(dj.Part):
        # NOTE: The table architecture makes sense as part table, but fetching from
        # BossDB might entail a subset of slices, which doesn't align with the design
        # goal of always ingesting master/parts at the same time
        definition = """ # Slice of a given volume
        -> Volume
        id: int # Nth voxel in slicing_dimension
        -> Zoom
        ---
        file_path : varchar(255) # filepath relative to root data directory
        """
