from collections import defaultdict, Mapping
from functools import partial
import itertools
import json
import logging
from pathlib import Path

import h5py
import numpy as np
import msgpack
import msgpack_numpy as mpn

from bluesky_kafka import RemoteDispatcher
from databroker import Broker
from event_model import DocumentRouter, RunRouter


logging.basicConfig(filename="worker.log")
logging.getLogger("bluesky.kafka").setLevel("INFO")


class MultiFilePacker(DocumentRouter):
    def __init__(self, directory, max_frames_per_file, handler_class):
        self.directory = directory
        self.max_frames_per_file = max_frames_per_file
        self.handler_class = handler_class
        self.resources = {}  # map resource_uid to resource dict
        self.datums = defaultdict(list)  # map resource_uid to list of datums
        self.start_doc = None

    def start(self, doc):
        print(
            f"New run detected (uid={doc['uid'][:8]}...). "
            f"Waiting to accumulate {self.max_frames_per_file} frames "
            f"or for the end of this run, whichever happens first."
        )
        self.start_doc = doc
        self.chunk_counter = itertools.count()
        # self.accum_pbar = tqdm(desc='accumulating data',
        #                        total=self.max_frames_per_file)

    def event(self, doc):
        print("event")

    def event_page(self, doc):
        print("event_page")

    def resource(self, doc):
        print("resource")
        self.resources[doc["uid"]] = doc

    def datum(self, doc):
        print("datum")
        self.datums[doc["resource"]].append(doc)
        # Assume one datum == one frame. Can be more careful later.
        # self.accum_pbar.update(1)
        if len(self.datums) == self.max_frames_per_file:
            self.export()

    def stop(self, doc):
        print("stop")
        # Export even if we haven't reached the limit yet.
        # No file should bridge across more than one run.
        if self.datums:
            # self.accum_pbar.update(self.max_frames_per_file)
            print("End of run detected. Forced export now.")
            self.export()

    def export(self):
        # Read in the images and stack them up.
        # for resource_uid, datums in tqdm(self.datums.items()):
        for resource_uid, datums in self.datums.items():
            resource = self.resources[resource_uid]
            images = []
            rpath = Path(resource["root"]) / Path(resource["resource_path"])
            handler = self.handler_class(rpath=rpath, **resource["resource_kwargs"])
            for datum in datums:
                image = handler(**datum["datum_kwargs"])
                images.append(image)
        image_stack = np.stack(images)
        self.datums.clear()
        self.resources.clear()

        # Write the HDF5 file.
        md = self.start_doc
        i = next(self.chunk_counter)
        filename = (
            f"{md['uid'][:8]}_"
            f"chunk{i:02}_"
            f"{md.get('sample_name', 'sample_name_not_recorded')}"
            ".h5"
        )
        print(f"Writing {filename} with shape {image_stack.shape}...")
        filepath = Path(self.directory) / Path(filename)
        if os.path.exists(filepath):
            print(f"MultiFilePacker deleting existing file {filepath}")
            os.remove(filepath)
        with h5py.File(filepath) as f:
            f.create_dataset("data", data=image_stack)
        print(f"Write complete.")


class SingleFilePacker(DocumentRouter):
    def __init__(self, directory, stream_name=None, fields=None, timestamps=True, use_id=True):
        self.directory = directory
        self.stream_name = stream_name
        self.fields = fields
        self.timestamps = timestamps
        self.use_id = use_id

        self.f = None
        self.top_group = None

    def start(self, doc):
        print(f"New run detected (uid={doc['uid'][:8]}...). ")

        filename = (
            f"{doc['uid'][:8]}_"
            f"{doc.get('sample_name', 'sample_name_not_recorded')}"
            ".h5"
        )
        filepath = Path(self.directory) / Path(filename)

        if os.path.exists(filepath):
            print(f"deleting existing file {filepath}")
            os.remove(filepath)
        print(f"Creating {filepath}...")
        self.f = h5py.File(filepath, "w")
        if self.use_id:
            top_group_name = doc["uid"]
        else:
            top_group_name = 'data_' + doc['scan_id']
        self.top_group = self.f.create_group(top_group_name)
        _safe_attrs_assignment(self.top_group["start"], doc)

    def descriptor(self, doc):
        _safe_attrs_assignment(self.top_group["descriptor"], doc)

    def event(self, doc):
        print("event")

    def event_page(self, doc):
        print("event_page")

    def resource(self, doc):
        print("resource")

    def datum(self, doc):
        print("datum")

    def stop(self, doc):
        print("stop")
        _safe_attrs_assignment(self.top_group["stop"], doc)
        self.f.close()


def _safe_attrs_assignment(h5_group, mapping):
    mapping = _clean_dict(mapping)
    for key, value in mapping.items():
        # Special-case None, which fails too late to catch below.
        if value is None:
            value = 'None'
        # Try storing natively.
        try:
            h5_group.attrs[key] = value
        # Fallback: Save the repr, which in many cases can be used to
        # recreate the object.
        except TypeError:
            h5_group.attrs[key] = json.dumps(value)


def _clean_dict(mapping):
    mapping = dict(mapping)
    for k, v in list(mapping.items()):
        # Store dictionaries as JSON strings.
        if isinstance(v, Mapping):
            mapping[k] = _clean_dict(mapping[k])
            continue
        try:
            json.dumps(v)
        except TypeError:
            mapping[k] = str(v)
    return mapping


import os
from databroker.assets.handlers_base import HandlerBase
from databroker.assets.base_registry import DuplicateHandler
import fabio

# for backward compatibility, fpp was always 1 before Jan 2018
# global pilatus_fpp
# pilatus_fpp = 1

# this is used by the CBF file handler
from enum import Enum


class data_file_path(Enum):
    #gpfs = "/nsls2/xf16id1/data"
    gpfs = "/GPFS/xf16id/exp_path"
    ramdisk = "/exp_path"


class triggerMode(Enum):
    software_trigger_single_frame = 1
    software_trigger_multi_frame = 2
    external_trigger = 3
    fly_scan = 4
    # external_trigger_multi_frame = 5  # this is unnecessary, difference is fpp


global pilatus_trigger_mode
# global default_data_path_root
# global substitute_data_path_root
# global CBF_replace_data_path

pilatus_trigger_mode = triggerMode.software_trigger_single_frame

# assuming that the data files always have names with these extensions
image_size = {"SAXS": (1043, 981), "WAXS1": (619, 487), "WAXS2": (1043, 981)}


# if the cbf files have been moved already
# CBF_replace_data_path = False


class PilatusCBFHandler(HandlerBase):
    specs = {"AD_CBF"} | HandlerBase.specs
    froot = data_file_path.gpfs

    def __init__(self, rpath, template, filename, frame_per_point=1, initial_number=1):
        # if frame_per_point>1:
        print(f"Initializing CBF handler for {pilatus_trigger_mode} ...")
        if (
            pilatus_trigger_mode != triggerMode.software_trigger_single_frame
            and frame_per_point > 1
        ):
            # file name should look like test_000125_SAXS_00001.cbf, instead of test_000125_SAXS.cbf
            template = template[:-4] + "_%05d.cbf"

        self._template = template
        self._fpp = frame_per_point
        self._filename = filename
        self._initial_number = initial_number
        self._image_size = None
        self._default_path = os.path.join(rpath, "")
        self._path = ""

        for k in image_size:
            if template.find(k) >= 0:
                self._image_size = image_size[k]
        if self._image_size is None:
            raise Exception(
                f"Unrecognized data file extension in filename template: {template}"
            )

        for fr in data_file_path:
            if self._default_path.find(fr.value) == 0:
                self._dir = self._default_path[len(fr.value) :]
                return
        raise Exception(f"invalid file path: {self._default_path}")

    def update_path(self):
        # this is a workaround for data that are save in /exp_path then moved to /GPFS/xf16id/exp_path
        if self.froot not in data_file_path:
            raise Exception(f"invalid froot: {self.froot}")
        self._path = self.froot.value + self._dir
        print(f"updating path, will read data from {self._path} ...")

    def get_data(self, fn):
        """ the file may not exist
        """
        try:
            print(f"reading file {fn}")
            img = fabio.open(fn)
            data = img.data
            if data.shape != self._image_size:
                print(
                    f"got incorrect image size from {fn}: {data.shape}"
                )  # , return an empty frame instead.')
        except:
            print(f"could not read {fn}, return an empty frame instead.")
            data = np.zeros(self._image_size)
        # print(data.shape)
        return data

    def __call__(self, point_number):
        start = self._initial_number  # + point_number
        stop = start + 1
        ret = []
        print("CBF handler called: start=%d, stop=%d" % (start, stop))
        print("  ", self._initial_number, point_number, self._fpp)
        print("  ", self._template, self._path, self._initial_number)
        self.update_path()

        if (
            pilatus_trigger_mode == triggerMode.software_trigger_single_frame
            or self._fpp == 1
        ):
            fn = self._template % (self._path, self._filename, point_number + 1)
            ret.append(self.get_data(fn))
        elif pilatus_trigger_mode in [
            triggerMode.software_trigger_multi_frame,
            triggerMode.fly_scan,
        ]:
            for i in range(self._fpp):
                fn = self._template % (self._path, self._filename, point_number + 1, i)
                # data = self.get_data(fn)
                # print(f"{i}: {data.shape}")
                ret.append(self.get_data(fn))
        elif pilatus_trigger_mode == triggerMode.external_trigger:
            fn = self._template % (self._path, self._filename, start, point_number)
            ret.append(self.get_data(fn))

        return np.array(ret).squeeze()


db = Broker.named("lix")
db.reg.register_handler("AD_CBF", PilatusCBFHandler, overwrite=True)

d = RemoteDispatcher(
    topics=["lix.bluesky.documents"],
    bootstrap_servers="10.0.137.8:9092",
    group_id="lix.export.worker",
    consumer_config={"auto.offset.reset": "latest"},
    polling_duration=1.0,
    deserializer=partial(msgpack.unpackb, object_hook=mpn.decode),
)


def multi_file_packer_factory(name, doc):
    packer = MultiFilePacker(
        directory="/tmp/export_worker/multi_file/",
        max_frames_per_file=2,
        handler_class=PilatusCBFHandler,
    )
    print("created a MultiFilePacker")
    return [packer], []


multi_file_run_router = RunRouter(factories=[multi_file_packer_factory])


def single_file_packer_factory(name, doc):
    packer = SingleFilePacker(
        directory="/tmp/export_worker/single_file/",
    )
    print("created a SingleFilePacker")
    return [packer], []


single_file_run_router = RunRouter(
    factories=[single_file_packer_factory],
    handler_registry={"AD_CBF": PilatusCBFHandler},
    fill_or_fail=True
)

d.subscribe(multi_file_run_router)
d.subscribe(single_file_run_router)
print("Starting Packer...")
d.start()
