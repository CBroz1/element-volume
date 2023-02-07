import logging
from typing import Optional, Tuple

import numpy as np
from datajoint.errors import DataJointError
from PIL import Image
from tqdm.auto import tqdm

from ..readers import BossDBInterface

logger = logging.getLogger("datajoint")


def bossdb_upload(
    url: str,
    data_dir: str,  # Local absolute path
    voxel_size: Tuple[int, int, int],  # voxel size in ZYX order
    voxel_units: str,  # The size units of a voxel
    shape_zyx: Tuple[int, int, int],
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

    url_exist = bool(BossDBInterface(url))  # returns false if not exist

    if not overwrite and url_exist:
        logger.warning(
            f"Dataset exists already exists at {url}\n"
            + " To overwrite, set `overwrite` to True"
        )
        return

    # Calculate the number of Z slices in the stack:
    image_paths = sorted(data_dir.glob("*" + data_extension))
    if not image_paths:
        raise DataJointError(
            "No images found in the specified directory "
            + f"{data_dir}/*{data_extension}."
        )

    boss_dataset = BossDBInterface(
        url,
        description="Uploaded via DataJoint",
        extents=shape_zyx,
        dtype=dtype,
        voxel_size=voxel_size,
        voxel_unit=voxel_units,
        create_new=not url_exist,  # If the url does not exist, we create new
        source_channel=source_channel if source_channel else None,
    )

    # Iterate in groups of upload_increment. If remainder, uploaded separately.
    for i in tqdm(range(0, shape_zyx[0], upload_increment)):
        if i + upload_increment > shape_zyx[0]:
            # We're at the end of the stack, so upload the remaining images.
            images = [Image.open(path) for path in image_paths[i : shape_zyx[0]]]
        else:
            images = [
                Image.open(path) for path in image_paths[i : i + upload_increment]
            ]
        stacked = np.stack([np.array(image, dtype=dtype) for image in images], axis=0)

        retry_count = 0

        while True:
            try:
                boss_dataset[
                    i : i + stacked.shape[0],
                    0 : stacked.shape[1],
                    0 : stacked.shape[2],
                ] = stacked
                break
            except Exception as e:
                logger.error(f"Error uploading chunk {i}-{i + stacked.shape[0]}: {e}")
                retry_count += 1
                if retry_count > retry_max:
                    raise e
                logger.info(f"Retrying increment {i} ...{retry_count}/{retry_max}")
                continue
