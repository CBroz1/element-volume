import logging
import os
from pathlib import Path
from typing import Optional, Tuple, Union

import numpy as np
from element_interface.utils import find_full_path
from intern import array
from PIL import Image
from PIL.Image import _fromarray_typemap
from requests import HTTPError

from .. import volume
from ..bossdb import BossDBURLs

logger = logging.getLogger("datajoint")


class BossDBInterface(array):
    def __init__(
        self,
        channel: Union[Tuple, str],
        session_key: Optional[dict] = None,
        volume_id: Optional[str] = None,
        **kwargs,
    ) -> None:

        try:
            super().__init__(channel=channel, **kwargs)
            self._exists = True
        except HTTPError as e:
            if e.response.status_code == 404 and not kwargs.get("create_new", False):
                self._exists = False
                return
            else:
                raise e

        self._session_key = session_key or dict()

        # If not passed resolution or volume IDs, use the following defaults:
        self._volume_key = dict(
            volume_id=volume_id or self.collection_name + "/" + self.experiment_name,
            resolution_id=self.resolution,
        )

    @property
    def exists(self):
        return self._exists

    def _infer_session_dir(self):
        root_dir = volume.get_vol_root_data_dir()[0]
        inferred_dir = (
            f"{self.collection_name}/{self.experiment_name}/{self.channel_name}/"
        )
        os.makedirs(Path(root_dir) / inferred_dir, exist_ok=True)
        return inferred_dir

    def _import_resolution(self, skip_duplicates=True):
        volume.Resolution.insert1(
            dict(
                resolution_id=self.resolution,  # integer 0-6
                voxel_unit=self.voxel_unit,  # axis order is either ZYX or XYZ
                voxel_z_size=self.voxel_size[0 if self.axis_order[0] == "Z" else 2],
                voxel_y_size=self.voxel_size[1],
                voxel_x_size=self.voxel_size[2 if self.axis_order[0] == "Z" else 0],
            ),
            skip_duplicates=skip_duplicates,
        )

    def _import_volume(self, data: np.ndarray = None, skip_duplicates=True):
        volume.Volume.insert1(
            dict(
                **self._session_key,
                **self._volume_key,
                z_size=self.shape[0 if self.axis_order[0] == "Z" else 2],
                y_size=self.shape[1],
                x_size=self.shape[2 if self.axis_order[0] == "Z" else 0],
                channel=self.channel_name,
                collection_experiment=f"{self.collection_name}_{self.experiment_name}",
                url=f"bossdb://{self._channel.get_cutout_route()}",
                volume_data=data,
            ),
            skip_duplicates=skip_duplicates,
        )

    def _import_segmentation(self, data: np.ndarray = None, skip_duplicates=True):
        volume.Segmentation.insert1(
            dict(**self._volume_key, segmentation_data=data),
            skip_duplicates=skip_duplicates,
            allow_direct_insert=True,
        )

    def _string_to_slice_key(self, string_key: str) -> Tuple:
        output = tuple()
        items = string_key.strip("[]").split(",")
        for index, item in enumerate(items):
            if item == ":":  # select all for dimension
                start, stop = (0, self.shape[index])
            elif ":" in item:  # select slice of dimension
                start, stop = list(map(int, item.split(":")))
            else:  # select a single slice
                start = int(item)
                stop = start + 1
            output = (*output, slice(start, stop))
        if len(output) == 1:  # If only on dimension provided, assume Z
            if self.axis_order[0] == "Z":
                return (output[0], slice(0, self.shape[1]), slice(0, self.shape[1]))
            else:
                return (slice(0, self.shape[0]), slice(0, self.shape[1]), output[0])
        return output

    def _slice_key_to_string(self, slice_key: Tuple[Union[int, slice]]) -> str:
        outputs = []
        for item in slice_key:
            if item.stop == item.start + 1:
                outputs.append(f"{item.start}")
            else:
                outputs.append(f"{item.start}:{item.stop}")
        return "[" + ",".join(outputs) + "]"

    def _download_slices(
        self,
        slice_key: Tuple[Union[int, slice]],
        data: np.ndarray,
        extension: str = ".png",
        image_mode: str = None,
    ):

        xs, ys, zs = self._normalize_key(key=slice_key)
        zoom = f"ZoomX{xs[0]}-{xs[1]}_Y{ys[0]}-{ys[1]}"

        # If associated session, use that dir. Else infer and mkdir
        if self._session_key:
            session_path = volume.get_session_directory(self._session_key)
        else:
            session_path = self._infer_session_dir()
        file_name = f"Res{self.resolution}_{zoom}_Z%d{extension}"
        file_path = find_full_path(volume.get_vol_root_data_dir(), session_path)
        file_path_full = str(file_path / file_name)

        if len(data.shape) == 1:  # if getitem returned single array, try unwrapping
            data = data[0]
        if len(data.shape) == 2:  # getitem returned single z-slice
            data = data[np.newaxis, :]

        for z in range(zs[0], zs[1]):
            # Z is used as absolute reference within dataset.
            # When saving data, 0-indexed based on slices fetched
            Image.fromarray(data[z - zs[0]], mode=image_mode).save(file_path_full % z)
        logger.info(f"Saved Z-slices {zs[0]} to {zs[1]}:\n{file_path}/")

    def insert_channel_as_url(self, data_channel="Volume", skip_duplicates=True):
        collection_key = dict(
            collection_experiment=self.collection_name + "_" + self.experiment_name
        )
        with BossDBURLs.connection.transaction:
            BossDBURLs.insert1(collection_key, skip_duplicates=skip_duplicates)
            getattr(BossDBURLs, data_channel).insert1(
                dict(
                    url=f"bossdb://{self._channel.get_cutout_route()}", **collection_key
                ),
                skip_duplicates=skip_duplicates,
            )

    def load_data_into_element(
        self,
        table: str = "Volume",
        slice_key: Union[Tuple[Union[int, slice]], str] = "[:]",  # Default full data
        save_images: bool = False,
        save_ndarray: bool = False,
        extension: str = ".png",
        skip_duplicates=False,
        image_mode=None,
    ):
        # NOTE: By accepting a slice here, we could download pngs and/or store ndarrays
        # that are a subset of the full volume with x and y start/stop limits. These
        # limits are not noted as part of the ndarray insert, but are tracked via
        # filename for images. We could (a) prevent loading partial volumes or (b) add
        # fields/tables to track this information. I previously included a Zoom table

        if isinstance(slice_key, str):
            slice_key = self._string_to_slice_key(slice_key)

        data = self.__getitem__(key=slice_key) if save_images or save_ndarray else None

        if (
            save_images
            and not image_mode
            and ((1, 1), str(data.dtype)) not in _fromarray_typemap
        ):
            image_mode_options = set(i[0] for i in _fromarray_typemap.values())
            raise ValueError(
                "Datatype is not supported for saving. Please select one of the "
                + f"following and pass it as `image_mode`: {image_mode_options}\n"
                + "See also docs for PIL.Image.fromarray"
            )

        self._import_resolution()

        if table == "Volume":
            self._import_volume(
                data=data if save_ndarray else None, skip_duplicates=skip_duplicates
            )
        elif table == "Segmentation":
            self._import_segmentation(
                data=data if save_ndarray else None, skip_duplicates=skip_duplicates
            )
        elif table == "Connectome":
            raise ValueError("BossDB API does not yet support fetching connectome.")

        if save_images:
            self._download_slices(slice_key, data, extension, image_mode)
