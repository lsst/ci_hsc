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
from collections import namedtuple

from lsst.utils import getPackageDir
from lsst.skymap import DiscreteSkyMap


class SingleEpochDataItem(namedtuple("SingleEpochDataItem", ("visit", "detector"))):

    def getDataId(self, dimensions, gen=2):
        if gen == 2:
            r = dict(visit=self.visit, ccd=self.detector)
        elif gen == 3:
            r = dict(instrument="HSC", detector=self.detector)
            if "Exposure" in dimensions:
                r["exposure"] = self.visit
            else:
                r["visit"] = self.visit
        else:
            raise ValueError("'gen' must be 2 or 3")
        return r


class CoaddDataItem(namedtuple("CoaddDataItem", ("tract", "px", "py", "filter"))):

    skymap = None

    def getDataId(self, dimensions, gen=2):
        if gen == 2:
            r = dict(tract=self.tract, patch=f"{self.px},{self.py}")
            if "AbstractFilter" in dimensions:
                r["filter"] = self.filter
        elif gen == 3:
            skyMap = self.getSkyMap()
            tractInfo = skyMap[self.tract]
            patchInfo = tractInfo.getPatchInfo((self.px, self.py))
            r = dict(skymap="ci_hsc", tract=self.tract, patch=tractInfo.getSequentialPatchIndex(patchInfo))
            if "AbstractFilter" in dimensions:
                r["abstract_filter"] = self.filter[-1].tolower()
        else:
            raise ValueError("'gen' must be 2 or 3")
        return r

    @classmethod
    def getSkyMap(cls):
        if cls.skymap is None:
            config = DiscreteSkyMap.ConfigClass()
            config.load(os.path.join(getPackageDir("ci_hsc"), "skymap.py"))
            cls.skymap = DiscreteSkyMap(config=config)
        return cls.skymap


DATA = {
    CoaddDataItem(tract=0, px=5, py=4, filter="HSC-R"): {
        SingleEpochDataItem(visit=903334, detector=16),
        SingleEpochDataItem(visit=903334, detector=22),
        SingleEpochDataItem(visit=903334, detector=23),
        SingleEpochDataItem(visit=903334, detector=100),
        SingleEpochDataItem(visit=903336, detector=17),
        SingleEpochDataItem(visit=903336, detector=24),
        SingleEpochDataItem(visit=903338, detector=18),
        SingleEpochDataItem(visit=903338, detector=25),
        SingleEpochDataItem(visit=903342, detector=4),
        SingleEpochDataItem(visit=903342, detector=10),
        SingleEpochDataItem(visit=903342, detector=100),
        SingleEpochDataItem(visit=903344, detector=0),
        SingleEpochDataItem(visit=903344, detector=5),
        SingleEpochDataItem(visit=903344, detector=11),
        SingleEpochDataItem(visit=903346, detector=1),
        SingleEpochDataItem(visit=903346, detector=6),
        SingleEpochDataItem(visit=903346, detector=12),
    },
    CoaddDataItem(tract=0, px=5, py=4, filter="HSC-I"): {
        SingleEpochDataItem(visit=903986, detector=16),
        SingleEpochDataItem(visit=903986, detector=22),
        SingleEpochDataItem(visit=903986, detector=23),
        SingleEpochDataItem(visit=903986, detector=100),
        SingleEpochDataItem(visit=904014, detector=1),
        SingleEpochDataItem(visit=904014, detector=6),
        SingleEpochDataItem(visit=904014, detector=12),
        SingleEpochDataItem(visit=903990, detector=18),
        SingleEpochDataItem(visit=903990, detector=25),
        SingleEpochDataItem(visit=904010, detector=4),
        SingleEpochDataItem(visit=904010, detector=10),
        SingleEpochDataItem(visit=904010, detector=100),
        SingleEpochDataItem(visit=903988, detector=16),
        SingleEpochDataItem(visit=903988, detector=17),
        SingleEpochDataItem(visit=903988, detector=23),
        SingleEpochDataItem(visit=903988, detector=24),
    },
}
