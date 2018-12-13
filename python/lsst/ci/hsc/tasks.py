# This file is part of ci_hsc.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
from collections import OrderedDict

from lsst.utils import getPackageDir
from lsst.daf.butler import DatasetType

PREEXISTING_DATASET_TYPES = {
    "raw", "flat", "bias", "dark", "fringe", "sky", "bfKernel",
    "ref_cat", "brightObjectMask", "camera",
    "transmission_sensor", "transmission_optics", "transmission_filter", "transmission_atmosphere",
}


def toDict(*items):
    result = OrderedDict()
    for item in items:
        result[item.name] = item
    return result


DATASET_TYPES = toDict(
    DatasetType("icSrc_schema", dimensions=(), storageClass="SourceCatalog"),
    DatasetType("src_schema", dimensions=(), storageClass="SourceCatalog"),
    DatasetType("raw", dimensions=("Detector", "Exposure"), storageClass="ExposureU"),
    DatasetType("flat", dimensions=("ExposureRange", "PhysicalFilter"), storageClass="MaskedImageF"),
    DatasetType("bias", dimensions=("ExposureRange",), storageClass="ImageF"),
    DatasetType("dark", dimensions=("ExposureRange",), storageClass="ImageF"),
    DatasetType("calexp", dimensions=("Detector", "Visit"), storageClass="ExposureF"),
    DatasetType("calexpBackground", dimensions=("Detector", "Visit"), storageClass="Background"),
    DatasetType("src", dimensions=("Detector", "Visit"), storageClass="SourceCatalog"),
    DatasetType("icSrc", dimensions=("Detector", "Visit"), storageClass="SourceCatalog"),
    DatasetType("srcMatch", dimensions=("Detector", "Visit"), storageClass="Catalog"),
    DatasetType("srcMatchFull", dimensions=("Detector", "Visit"), storageClass="Catalog"),
)


def normalizeDatasetType(datasetType):
    if not isinstance(datasetType, DatasetType):
        datasetType = DATASET_TYPES[datasetType]
    return datasetType


def stripAffixes(s, *, prefix=None, suffix=None, prefixRequired=False, suffixRequired=False):
    if prefix:
        if s.startswith(prefix):
            s = s[len(prefix):]
        elif prefixRequired:
            raise ValueError(f"Required prefix '{prefix}' not found on '{s}'")
    if suffix:
        if s.endswith(suffix):
            s = s[:-len(suffix)]
        elif suffixRequired:
            raise ValueError(f"Required suffix '{suffix}' not found on '{s}'")
    return s


def flattenPipelineTaskDatasetTypes(dictionary):
    """Unpack the result of a call to PipelineTask.get*DatasetTypes() into
    flat iteration over `DatasetType` instances.
    """
    for inner in dictionary.values():
        yield inner.datasetType


def predictFileName(butler, datasetType, *data, gen=2):
    dataId = dict()
    for d in data:
        dataId.update(d.getDataId(datasetType.dimensions, gen=gen))
    if gen == 2:
        return butler.getUri(datasetType.name, dataId, write=True)
    elif gen == 3:
        uri = butler.getUri(datasetType, dataId, predict=True)
        filename = stripAffixes(uri, prefix="file:///", suffix="#predicted", prefixRequired=True)
        return os.path.join(getPackageDir("ci_hsc"), "DATA", filename)
    return filename


class TaskInfo:

    def __init__(self, name, executable=None, args=None, idArgFunc=None, task=None,
                 initInputs=(), initOutputs=(), inputs=(), outputs=(),
                 quantum=None, configName=None, metadataName=None, package="pipe_tasks"):
        self.name = name
        self.executable = (executable if executable is not None
                           else os.path.join(getPackageDir(package), "bin", f"{name}.py"))

        self.task = task
        if task is not None:
            config = task.ConfigClass()
            self.initInputs = set(flattenPipelineTaskDatasetTypes(task.getInitInputDatasetTypes(config)))
            self.initOutputs = set(flattenPipelineTaskDatasetTypes(task.getInitOutputDatasetTypes(config)))
            self.inputs = set(flattenPipelineTaskDatasetTypes(task.getInputDatasetTypes(config)))
            self.outputs = set(flattenPipelineTaskDatasetTypes(task.getOutputDatasetTypes(config)))
            self.quantum = set(config.quantum.dimensions)
        else:
            self.initInputs = set(normalizeDatasetType(dst) for dst in initInputs)
            self.initOutputs = set(normalizeDatasetType(dst) for dst in initOutputs)
            self.inputs = set(normalizeDatasetType(dst) for dst in inputs)
            self.outputs = set(normalizeDatasetType(dst) for dst in outputs)
            self.quantum = set(quantum)

        def defaultIdArgFunc(*data):
            dataId = dict()
            for d in data:
                dataId.update(d.getDataId(self.quantum, gen=2))
            return "--id {}".format(" ".join(f"{k}={v}" for k, v in dataId.items()))

        if idArgFunc is None:
            idArgFunc = defaultIdArgFunc
        self.idArgFunc = idArgFunc
        self.args = args if args is not None else ""

        if configName is None:
            configName = f"{name}_config"
        if configName:
            self.initOutputs.add(DatasetType(configName, dimensions=(), storageClass="Config"))

        if metadataName is None:
            metadataName = f"{name}_metadata"
        if metadataName:
            self.outputs.add(DatasetType(metadataName, dimensions=self.quantum, storageClass="PropertySet"))

    def makeInitTarget(self, env, butler, *, inputs=(), outputs=(),
                       root="DATA", rerun="ci_hsc", prefix="python"):
        inputs = list(inputs)
        inputs.extend(predictFileName(butler, datasetType, gen=2) for datasetType in self.initInputs)
        outputs = list(outputs)
        outputs.extend(predictFileName(butler, datasetType, gen=2) for datasetType in self.initOutputs)
        cmd = f"{prefix} {self.executable} {root} --rerun {rerun} {self.args}"
        return env.Command(outputs, inputs, [cmd])

    def makeRunTarget(self, env, butler, *data, inputs=(), outputs=(),
                      root="DATA", rerun="ci_hsc", prefix="python"):
        inputs = list(inputs)
        inputs.extend(predictFileName(butler, datasetType, gen=2) for datasetType in self.initInputs)
        inputs.extend(predictFileName(butler, datasetType, gen=2) for datasetType in self.initOutputs)
        inputs.extend(predictFileName(butler, datasetType, *data, gen=2)
                      for datasetType in self.inputs
                      if datasetType.name not in PREEXISTING_DATASET_TYPES)
        outputs = list(outputs)
        outputs.extend(predictFileName(butler, datasetType, *data, gen=2)
                       for datasetType in self.outputs)
        idArgs = self.idArgFunc(*data)
        cmd = f"{prefix} {self.executable} {root} --rerun {rerun} {self.args} {idArgs}"
        return env.Command(outputs, inputs, [cmd])


PIPELINE = toDict(
    TaskInfo(
        name="processCcd",
        initOutputs=("src_schema", "icSrc_schema"),
        inputs=("raw", "flat", "bias", "dark"),
        outputs=("calexp", "calexpBackground", "src", "icSrc", "srcMatch", "srcMatchFull"),
        quantum=("Visit", "Detector"),
    ),
)
