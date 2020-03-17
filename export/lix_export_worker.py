from collections import defaultdict
from collections.abc import Mapping
from functools import partial
import itertools
import json
import logging
import os
from pathlib import Path
import pprint

import h5py
import numpy as np
import msgpack
import msgpack_numpy as mpn

from bluesky_kafka import RemoteDispatcher
from databroker import Broker
from event_model import DocumentRouter, RunRouter


if os.path.exists("worker.log"):
    os.remove("worker.log")
logging.basicConfig(filename="worker.log")
logging.getLogger("bluesky.kafka").setLevel("INFO")
logging.getLogger("lix").setLevel("DEBUG")


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
        #print("event")
        pass

    def event_page(self, doc):
        #print("event_page")
        pass

    def resource(self, doc):
        #print("resource")
        self.resources[doc["uid"]] = doc

    def datum(self, doc):
        #print("datum")
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
        #print(f"Writing {filename} with shape {image_stack.shape}...")
        filepath = Path(self.directory) / Path(filename)
        if os.path.exists(filepath):
            #print(f"MultiFilePacker deleting existing file {filepath}")
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

        self.event_page_doc_count = 0
        self.event_doc_count = 0

        self.filepath = None
        self.scan_group_name = None

        self.descriptor_uid_to_stream_name = dict()
        
        self.log = logging.getLogger("lix")

    def start(self, doc):
        self.log.info("New run detected uid=%s", doc["uid"])
        self.log.debug(pprint.pformat(doc))
        filename = (
            f"{doc['uid'][:8]}_"
            f"{doc.get('sample_name', 'sample_name_not_recorded')}"
            ".h5"
        )
        self.filepath = Path(self.directory) / Path(filename)
        if os.path.exists(self.filepath):
            self.log.warning("%s exists", self.filepath)
            self.log.warning("deleting %s", self.filepath)
            os.remove(self.filepath)

        # open filepath and create a top-level group
        # named with scan_id or uid
        if self.use_id:
            self.scan_group_name = doc["uid"]
        else:
            self.scan_group_name = str(doc["scan_id"])

        self.log.info("opening file %s", self.filepath)
        with h5py.File(self.filepath, "w") as f:
            # create the top-level group for this scan
            if self.scan_group_name in f.keys():
                self.log.info("scan group %s already exists", self.scan_group_name)
            else:
                scan_group = f.create_group(self.scan_group_name)
                self.log.info("created scan group %s", scan_group)
                
    def descriptor(self, doc):
        self.log.info("descriptor name=%s", doc["name"])
        self.log.debug(pprint.pformat(doc))

        self.descriptor_uid_to_stream_name[doc["uid"]] = doc["name"]

        if not os.path.exists(self.filepath):
            raise FileNotFoundError(self.filepath)
        
        with h5py.File(self.filepath, "a") as f:
            h5_scan_group = f[self.scan_group_name]
            h5_scan_stream_group = h5_scan_group.create_group(doc["name"])
            h5_scan_stream_data_group = h5_scan_stream_group.create_group("data")
            h5_scan_stream_timestamps_group = h5_scan_stream_group.create_group("timestamps")
            h5_scan_stream_group.create_dataset(
                name="time",
                dtype="f8",
                shape=(0, ),
                maxshape=(None, ),
                chunks=(1, )
            )
            for ep_data_key, ep_data_info in doc["data_keys"].items():
                h5_scan_stream_timestamps_group.create_dataset(
                    name=ep_data_key,
                    dtype="f8",
                    shape=(0,),
                    maxshape=(None,),
                    chunks=(1,)
                )

                self.log.debug("creating a dataset for %s with data type %s", ep_data_key, ep_data_info["dtype"])
                if ep_data_info["dtype"] == "array":
                    # data_info["shape"] looks like [b, a, 0] but should be [0, a, b]
                    # this is a bug
                    h5_dataset_shape = ep_data_info["shape"].copy()
                    h5_dataset_shape.reverse()

                    self.log.debug("creating dataset %s with shape %s", ep_data_key, h5_dataset_shape)
                    h5_scan_stream_data_dataset = h5_scan_stream_data_group.create_dataset(
                        name=ep_data_key,
                        dtype="i4",  # TODO: don't do that
                        # start with axis 0 size 0 because it will be resized
                        # before every new array is stored
                        shape=h5_dataset_shape,
                        # here I want (None, a, b)
                        maxshape=(None, *h5_dataset_shape[1:]),
                        chunks=(1, *h5_dataset_shape[1:])
                    )
                elif ep_data_info["dtype"] == "string":
                    unicode_data_type = h5py.string_dtype()
                    h5_scan_stream_data_dataset = h5_scan_stream_data_group.create_dataset(
                        name=ep_data_key,
                        # data=data,
                        shape=(0,),
                        maxshape=(None,),
                        chunks=(1,),
                        dtype=unicode_data_type,
                        compression='gzip'
                    )
                elif ep_data_info["dtype"] == "integer":
                    ...
                elif ep_data_info["dtype"] == "number":
                    ...
                else:
                    raise Exception()

        self.log.debug("created dataset %s", h5_scan_stream_data_dataset)

    def event(self, doc):
        self.log.info("event")
        self.event_doc_count += 1

    def event_page(self, doc):
        self.log.info("event_page")
        self.log.debug(pprint.pformat(doc))
        self.log.debug("doc[data][pil1M_ext_image][0] shape: %s", doc["data"]["pil1M_ext_image"][0].shape)
        self.log.debug("doc[data][pilW1_ext_image][0] shape: %s", doc["data"]["pilW1_ext_image"][0].shape)
        self.log.debug("doc[data][pilW2_ext_image][0] shape: %s", doc["data"]["pilW2_ext_image"][0].shape)

        with h5py.File(self.filepath) as f:
            event_page_time = doc["time"]
            event_page_timestamps = doc["timestamps"]
            stream_name = self.descriptor_uid_to_stream_name[doc["descriptor"]]
            h5_scan_stream_data_group = f[self.scan_group_name][stream_name]["data"]
            for ep_data_key, ep_data in doc["data"].items():
                if ep_data_key in h5_scan_stream_data_group:
                    self.log.debug("found event_page data_key %s in h5 group %s", ep_data_key, h5_scan_stream_data_group)
                    ep_data_array = ep_data[0]
                    h5_data_array = h5_scan_stream_data_group[ep_data_key]

                    self.log.debug("%s has len() %s", h5_data_array, h5_data_array.len())
                    h5_data_array.resize((h5_data_array.shape[0]+1, *h5_data_array.shape[1:]))
                    if hasattr(ep_data_array, "shape"):
                        self.log.debug("event page data has shape %s", ep_data_array.shape)
                        h5_data_array[-1, :] = ep_data_array
                    else:
                        # not an array...
                        self.log.debug("event page data: %s", ep_data_array)
                        h5_data_array[-1] = ep_data_array

        self.event_page_doc_count += 1
        
    def resource(self, doc):
        self.log.info("resource")

    def datum(self, doc):
        self.log.info("datum")

    def stop(self, doc):
        self.log.info("stop")
        self.log.info("%d event page documents", self.event_page_doc_count)
        self.log.info("%d event documents", self.event_doc_count)


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
        directory="/tmp/export_worker/single_file/", use_id=False
    )
    print("created a SingleFilePacker")
    return [packer], []


single_file_run_router = RunRouter(
    factories=[single_file_packer_factory],
    handler_registry={"AD_CBF": PilatusCBFHandler},
    fill_or_fail=True
)

# getting unknown datum errors from multi_file_run_router
#d.subscribe(multi_file_run_router)
d.subscribe(single_file_run_router)
print("Starting Packer...")
d.start()
