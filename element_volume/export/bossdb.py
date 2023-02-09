import logging
from typing import Optional, Tuple

import numpy as np
from datajoint.errors import DataJointError
from intern.convenience.array import _BossDBVolumeProvider
from PIL import Image
from tqdm.auto import tqdm

from ..readers.bossdb import BossDBInterface

logger = logging.getLogger("datajoint")


def bossdb_upload(
    url: str,
    data_dir: str,  # Local absolute path
    voxel_size: Tuple[int, int, int],  # voxel size in ZYX order
    voxel_units: str,  # The size units of a voxel
    shape_zyx: Tuple[int, int, int],
    resolution: int = 0,
    raw_data: np.array = None,
    data_extension: Optional[str] = "",  # Can omit if uploading every file in dir
    upload_increment: Optional[int] = 32,  # How many z slices to upload at once
    retry_max: Optional[int] = 3,  # Number of retries to upload a single
    dtype: Optional[str] = "uint8",  # Data-type of the image data. "uint8" or "uint64"
    overwrite: Optional[bool] = False,  # Overwrite existing data
    source_channel: Optional[str] = None,  # What to name a new channel
):
    # TODO: Move comments to full docstring
    # upload_increment (int):  For best performance, use be a multiple of 16.
    #   With a lot of RAM, 64. If out-of-memory errors, decrease to 16. If issues
    #   persist, try 8 or 4.

    url_exist = BossDBInterface(url).exists
    if not overwrite and url_exist:
        logger.warning(
            f"Dataset exists already exists at {url}\n"
            + " To overwrite, set `overwrite` to True"
        )
        return

    boss_dataset = BossDBInterface(
        url,
        resolution=resolution,
        volume_provider=_BossDBVolumeProvider(),
        description="Uploaded via DataJoint",
        extents=shape_zyx,
        dtype=dtype,
        voxel_size=voxel_size,
        voxel_unit=voxel_units,
        create_new=not url_exist,  # If the url does not exist, create new
        source_channel=source_channel if source_channel else None,
    )
    """
    > /Users/cb/Documents/dev/intern/intern/service/boss/v1/project.py(759)create()
    757         req = self.get_request(resource, 'POST', 'application/json', url_prefix, auth, json=json)
    758
--> 759         prep = session.prepare_request(req)
    760         resp = session.send(prep, **send_opts)
    761

    ipdb> json
    {'name': 'CF_DataJointTest_test', 'description': 'Uploaded via DataJoint', 'x_start': 0, 'x_stop': 246, 'y_start': 0, 'y_stop': 246, 'z_start': 0, 'z_stop': 20, 'x_voxel_size': 0.5, 'y_voxel_size': 0.5, 'z_voxel_size': 1.0, 'voxel_unit': 'micrometers'}
    ipdb> # TypeError: Object of type int64 is not JSON serializable
    """

    if not raw_data:
        image_paths = sorted(data_dir.glob("*" + data_extension))
        if not image_paths:
            raise DataJointError(
                "No images found in the specified directory "
                + f"{data_dir}/*{data_extension}."
            )

    z_max = shape_zyx[0]
    for i in tqdm(range(0, z_max, upload_increment)):
        z_limit = min(i + upload_increment, z_max)  # whichever smaller incriment or end

        stack = (
            raw_data[i:z_limit]
            if raw_data
            else _np_from_images(image_paths, i, z_limit, dtype)
        )

        retry_count = 0

        while True:
            try:
                boss_dataset[
                    i : i + stack.shape[0],
                    0 : stack.shape[1],
                    0 : stack.shape[2],
                ] = stack
                break
            except Exception as e:
                logger.error(f"Error uploading chunk {i}-{i + stack.shape[0]}: {e}")
                retry_count += 1
                if retry_count > retry_max:
                    raise e
                logger.info(f"Retrying increment {i} ...{retry_count}/{retry_max}")
                continue


def _np_from_images(image_paths, i, z_limit, dtype):
    return np.stack(
        [
            np.array(image, dtype=dtype)
            for image in [Image.open(path) for path in image_paths[i:z_limit]]
        ],
        axis=0,
    )
