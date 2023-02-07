import logging
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Optional, Tuple, Union

import numpy as np
from element_interface.utils import find_full_path
from intern import array
from PIL import Image
from requests import HTTPError

from .. import volume

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
            _ = super().__init__(channel=channel, **kwargs)
        except HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"URL does not exist {channel}")
                return False
            else:
                raise e

        self._session_key = session_key or dict()

        # If not passed resolution or volume IDs, use the following defaults:
        self._volume_key = dict(
            volume_id=volume_id or self.collection_name + "/" + self.experiment_name,
            resolution_id=self.resolution,
        )

    def _infer_session_dir(self):
        root_dir = volume.get_vol_root_data_dir()
        if isinstance(root_dir, Sequence):
            root_dir = root_dir[0]
        inferred_dir = (
            f"{self.collection_name}/{self.experiment_name}/{self.channel_name}/"
        )
        os.makedirs(Path(root_dir) / inferred_dir, exist_ok=True)
        return inferred_dir

    def _import_resolution(self):
        volume.Resolution.insert1(
            dict(
                resolution_id=self.resolution,  # integer 0-6
                voxel_unit=self.voxel_unit,  # axis order is either ZYX or XYZ
                voxel_z_size=self.voxel_size[0 if self.axis_order[0] == "Z" else 2],
                voxel_y_size=self.voxel_size[1],
                voxel_x_size=self.voxel_size[2 if self.axis_order[0] == "Z" else 0],
            ),
            skip_duplicates=True,
        )

    def _import_volume(self, volume_id: str = None):
        if volume_id:
            self._volume_key.update(dict(volume_id=self.volume_id))

        volume.Volume.insert1(
            dict(
                **self._session_key,
                **self._volume_key,
                z_size=self.shape[0 if self.axis_order[0] == "Z" else 2],
                y_size=self.shape[1],
                x_size=self.shape[2 if self.axis_order[0] == "Z" else 0],
                channel=self.channel_name,
                url=self.url,
            ),
            skip_duplicates=True,
        )

    def _get_zoom_id(self, xs, ys):
        _shape = self.shape
        y_max, x_max = _shape[1:3] if self.axis_order[0] == "Z" else _shape[-2::1]
        if xs[0] == 0 and ys[0] == 0 and xs[1] == x_max and ys[1] == y_max:
            return "Full Image"
        else:
            zoom_id = f"X{xs[0]}-{xs[1]}_Y{ys[0]}-{ys[1]}"
            volume.Zoom.insert1(
                dict(
                    zoom_id=zoom_id,
                    first_start=xs[0],
                    first_end=xs[1],
                    second_start=ys[0],
                    second_end=ys[1],
                ),
                skip_duplicates=True,
            )
            return zoom_id

    def _fetch_slice_data(self, xs, ys, zs):
        cutout = self.volume_provider.get_cutout(
            self._channel, self.resolution, xs, ys, zs
        )
        if self.axis_order != self.volume_provider.get_axis_order():
            data: np.ndarray = np.swapaxes(cutout, 0, 2)
        else:
            data: np.ndarray = cutout
        # NOTE: does not collapse slice by dimension like array.__getitem___ for
        # convenience when loading into volume.Volume.Slice table
        return data

    def _string_to_slice_key(self, string_key: str) -> Tuple:
        output = tuple()
        items = string_key.strip("[]").split(",")
        for item in items:
            if ":" in item:
                start, stop = list(map(int, item.split(":")))
            else:
                start = int(item)
                stop = start + 1
            output = (*output, slice(start, stop))
        return output

    def _slice_key_to_string(self, slice_key: Tuple[Union[int, slice]]) -> str:
        outputs = []
        for item in slice_key:
            if item.stop == item.start + 1:
                outputs.apend(f"{item.start}")
            else:
                outputs.append(f"{item.start}:{item.stop}")
        return "[" + ",".join(outputs) + "]"

    def _download_slices(
        self,
        slice_key: Tuple[Union[int, slice]],
        extension: str = ".png",
    ):
        xs, ys, zs = self._normalize_key(key=slice_key)
        data = self._fetch_slice_data(xs, ys, zs)
        zoom_id = self._get_zoom_id(xs, ys)

        # If dir provided by get_session, use that. Else infer and mkdir
        session_path = (
            volume.get_session_directory(self._session_key) or self._infer_session_dir()
        )
        file_name = f"Res{self.resolution}_Zoom{zoom_id}_Z%d{extension}"
        file_path_full = str(
            find_full_path(volume.get_vol_root_data_dir(), session_path) / file_name
        )

        for z in range(zs[0], zs[1]):
            # Z is used as absolute reference within dataset
            # When saving data, 0-indexed based on slices fetched
            Image.fromarray(data[z - zs[0]]).save(file_path_full % z)

    def download(
        self,
        slice_key: Union[Tuple[Union[int, slice]], str],
        save_images: bool = False,
        extension: str = ".png",
    ):
        if isinstance(slice_key, str):
            slice_key = self._string_to_slice_key(slice_key)
        self._import_resolution()
        self._import_volume()
        if save_images:
            self._download_slices(slice_key, extension)
