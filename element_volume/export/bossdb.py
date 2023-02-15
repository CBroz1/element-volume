import logging
from typing import Optional, Tuple

import numpy as np
from datajoint.errors import DataJointError
from intern.convenience.array import _parse_bossdb_uri
from intern.remote.boss import BossRemote
from intern.resource.boss.resource import (
    ChannelResource,
    CollectionResource,
    CoordinateFrameResource,
    ExperimentResource,
)
from PIL import Image
from requests import HTTPError
from tqdm.auto import tqdm

from ..readers.bossdb import BossDBInterface

logger = logging.getLogger("datajoint")


class BossDBUpload:
    def __init__(
        self,
        url: str,
        data_dir: str,  # Local absolute path
        voxel_size: Tuple[int, int, int],  # voxel size in ZYX order
        voxel_units: str,  # The size units of a voxel
        shape_zyx: Tuple[int, int, int],
        resolution: int = 0,
        raw_data: np.array = None,
        data_extension: Optional[str] = "",  # Can omit if uploading every file in dir
        upload_increment: Optional[int] = 16,  # How many z slices to upload at once
        retry_max: Optional[int] = 3,  # Number of retries to upload a single
        dtype: Optional[str] = None,  # type of the image data. e.g., uint8, uint64
        overwrite: Optional[bool] = False,  # Overwrite existing data
    ):
        # TODO: Move comments to full docstring
        # upload_increment (int):  For best performance, use be a multiple of 16.
        #   With a lot of RAM, 64. If out-of-memory errors, decrease to 16. If issues
        #   persist, try 8 or 4.

        # int/float typing bc upload had issues with json serializing np.int64
        self._url = url
        self.url_bits = _parse_bossdb_uri(url)
        self._data_dir = data_dir
        self._voxel_size = tuple(float(i) for i in voxel_size)
        self._voxel_units = voxel_units
        self._shape_zyx = tuple(int(i) for i in shape_zyx)
        self._resolution = resolution
        self._raw_data = raw_data
        self._data_extension = data_extension
        self._upload_increment = upload_increment
        self._retry_max = retry_max
        self._overwrite = overwrite
        self.description = "Uploaded via DataJoint"
        self._resources = dict()

        self.url_exists = BossDBInterface(self._url).exists
        if not overwrite and self.url_exists:
            logger.warning(
                f"Dataset exists already exists at {self._url}\n"
                + " To overwrite, set `overwrite` to True"
            )
            return

        if not self.url_exists:
            self.try_create_new()

        if self._raw_data is None:  # 'is None' bc np.array as ambiguous truth value
            self._image_paths = self.fetch_images()
            self._dtype = dtype or None
        else:
            self._dtype = dtype or self._raw_data.dtype

    def fetch_images(self):
        image_paths = sorted(self._data_dir.glob("*" + self._data_extension))
        if not image_paths:
            raise DataJointError(
                "No files found in the specified directory "
                + f"{self._data_dir}/*{self._data_extension}."
            )
        return image_paths

    @property
    def dataset(self):
        return BossDBInterface(
            self._url,
            extents=self._shape_zyx,
            dtype=self._dtype,
            resolution=self._resolution,
            voxel_size=self._voxel_size,
            voxel_unit=self._voxel_units,
            create_new=True,  # not self.url_exists,  # If the url does not exist, create new
            source_channel=self.url_bits.channel,
            # volume_provider=_BossDBVolumeProvider(),
            # description=self.description,
        )

    def upload(self):
        z_max = self._shape_zyx[0]
        for i in tqdm(range(0, z_max, self._upload_increment)):
            # whichever smaller increment or end
            z_limit = min(i + self._upload_increment, z_max)

            stack = (
                self._raw_data[i:z_limit]
                if self._raw_data is not None
                else self._np_from_images(i, z_limit)
            )

            if not stack.flags["C_CONTIGUOUS"]:
                stack = np.ascontiguousarray(stack)

            stack_shape = stack.shape

            retry_count = 0

            while True:
                try:
                    self.dataset[
                        i : i + stack_shape[0],
                        0 : stack_shape[1],
                        0 : stack_shape[2],
                    ] = stack
                    break
                except Exception as e:
                    logger.error(f"Error uploading chunk {i}-{i + stack_shape[0]}: {e}")
                    retry_count += 1
                    if retry_count > self._retry_max:
                        raise e
                    logger.info(
                        f"Retrying increment {i}...{retry_count}/{self._retry_max}"
                    )
                    continue
        # 'Create cutout failed on CalciumImaging, got HTTP response: (400) - {"status":
        # 400, "code": 2002, "message": "Failed to unpack data. Verify the datatype of
        # your POSTed data and xyz dimensions used in the POST URL."}'

    def _np_from_images(self, i, z_limit):
        return np.stack(
            [
                np.array(image, dtype=self._dtype)
                for image in [Image.open(path) for path in self._image_paths[i:z_limit]]
            ],
            axis=0,
        )

    @property
    def resources(self):
        # Default resources for creating channels
        coord_name = f"CF_{self.url_bits.collection}_{self.url_bits.experiment}"
        if not self._resources:
            self._resources = dict(
                collection=CollectionResource(
                    name=self.url_bits.collection, description=self.description
                ),
                coord_frame=CoordinateFrameResource(
                    name=coord_name,
                    description=self.description,
                    x_start=0,
                    x_stop=self._shape_zyx[2],
                    y_start=0,
                    y_stop=self._shape_zyx[1],
                    z_start=0,
                    z_stop=self._shape_zyx[0],
                    x_voxel_size=self._voxel_size[2],
                    y_voxel_size=self._voxel_size[1],
                    z_voxel_size=self._voxel_size[0],
                ),
                experiment=ExperimentResource(
                    name=self.url_bits.experiment,
                    collection_name=self.url_bits.collection,
                    coord_frame=coord_name,
                    description=self.description,
                ),
                channel_resource=ChannelResource(
                    name=self.url_bits.channel,
                    collection_name=self.url_bits.collection,
                    experiment_name=self.url_bits.experiment,
                    type="image",
                    description=self.description,
                    datatype=self._dtype,
                ),
                channel=ChannelResource(
                    name=self.url_bits.channel,
                    collection_name=self.url_bits.collection,
                    experiment_name=self.url_bits.experiment,
                    type="image",
                    description=self.description,
                    datatype=self._dtype,
                    sources=[],
                ),
            )
        return self._resources

    def try_create_new(self):
        remote = BossRemote()

        # Make collection
        _ = self._get_or_create(remote=remote, obj=self.resources["collection"])

        # Make coord frame
        true_coord_frame = self._get_or_create(
            remote=remote, obj=self.resources["coord_frame"]
        )

        # Set Experiment based on coord frame
        experiment = self.resources["experiment"]
        experiment.coord_frame = true_coord_frame.name
        _ = self._get_or_create(remote=remote, obj=experiment)

        # Set channel based on resource
        channel_resource = self._get_or_create(
            remote=remote, obj=self.resources["channel_resource"]
        )
        channel = self.resources["channel"]
        channel.sources = [channel_resource.name]
        _ = self._get_or_create(remote=remote, obj=channel)

    def _get_or_create(self, remote, obj):
        try:
            result = remote.get_project(obj)
        except HTTPError:
            logger.info(f"Creating {obj.name}")
            result = remote.create_project(obj)
        return result
