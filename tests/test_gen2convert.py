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
import unittest

import lsst.utils.tests
import lsst.afw.image.testUtils  # noqa; injects test methods into TestCase
import lsst.meas.algorithms
from lsst.utils import getPackageDir
from lsst.daf.butler import Butler, DataId
from lsst.daf.butler.sql import SingleDatasetQueryBuilder
from lsst.daf.persistence import Butler as Butler2
from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam
from lsst.pipe.tasks.objectMasks import ObjectMaskCatalog


REPO_ROOT = os.path.join(getPackageDir("ci_hsc"), "DATA")


class Gen2ConvertTestCase(lsst.utils.tests.TestCase):

    def setUp(self):
        self.butler = Butler(REPO_ROOT, run="shared/ci_hsc")

    def tearDown(self):
        del self.butler

    def testImpliedDimensions(self):
        """Test that implied dimensions are expanded properly when populating
        the Dataset table.
        """
        # All of the dataset types below have Visit or Exposure in their
        # dimensions, which means PhysicalFilter and AbstractFilter are
        # implied. dimensions for them.  Those should be non-null and
        # consistent.
        sql = """
            SELECT physical_filter, abstract_filter
            FROM dataset
            WHERE dataset_type_name IN (
                'raw', 'calexp', 'icExp', 'src', 'icSrc',
                'deepCoadd_directWarp', 'deepCoadd_psfMatchedWarp'
            )
            """
        count = 0
        for row in self.butler.registry.query(sql):
            if row["physical_filter"] == "HSC-R":
                self.assertEqual(row["abstract_filter"], "r")
            elif row["physical_filter"] == "HSC-I":
                self.assertEqual(row["abstract_filter"], "i")
            else:
                self.fail("physical_filter not in ('HSC-R', 'HSC-I')")
            count += 1
        self.assertGreater(count, 0)

    def testObservationPacking(self):
        """Test that packing Visit+Detector into an integer in Gen3 generates
        the same results as in Gen2.
        """
        butler2 = Butler2(os.path.join(REPO_ROOT, "rerun", "ci_hsc"))
        for visit, detector in [(903334, 16), (903338, 25), (903986, 100)]:
            dataId2 = {"visit": visit, "ccd": detector}
            dataId3 = self.butler.registry.expandDataId(visit=visit, detector=detector, instrument="HSC")
            self.assertEqual(butler2.get("ccdExposureId", dataId2),
                             self.butler.registry.packDataId("visit_detector", dataId3))

    def testSkyMapPacking(self):
        """Test that packing Tract+Patch into an integer in Gen3 works and is
        self-consistent.

        Note that this packing does *not* use the same algorithm as Gen2 and
        hence generates different IDs, because the Gen2 algorithm is
        problematically tied to the *default* SkyMap for a particular camera,
        rather than the SkyMap actually used.
        """
        # SkyMap used by ci_hsc has only one tract, so the test coverage in
        # that area isn't great.  That's okay because that's tested in SkyMap;
        # what we care about here is that the converted repo has the necessary
        # metadata to construct and use these packers at all.
        for patch in [0, 43, 52]:
            dataId = self.butler.registry.expandDataId(skymap="ci_hsc", tract=0, patch=patch,
                                                       abstract_filter='r')
            packer1 = self.butler.registry.makeDataIdPacker("tract_patch", dataId)
            packer2 = self.butler.registry.makeDataIdPacker("tract_patch_abstract_filter", dataId)
            self.assertNotEqual(packer1.pack(dataId), packer2.pack(dataId))
            self.assertEqual(packer1.unpack(packer1.pack(dataId)),
                             DataId(dataId, dimensions=packer1.dimensions.required))
            self.assertEqual(packer2.unpack(packer2.pack(dataId)), dataId)
            self.assertEqual(packer1.pack(dataId, abstract_filter='i'), packer1.pack(dataId))
            self.assertNotEqual(packer2.pack(dataId, abstract_filter='i'), packer2.pack(dataId))

    def testRawFilters(self):
        """Test that raw data has the Filter component set.
        """
        # Note that the 'r' and 'i' values here look like Gen3 abstract_filter
        # values, but they're something weird in between abstract and physical
        # filters; if we had HSC-R2 data, the corresponding value would be
        # 'r2', not just 'r'.  We need that to be compatible with Gen2 usage
        # of the afw.image.Filter system.
        rawR = self.butler.get("raw", instrument="HSC", exposure=903334, detector=16)
        self.assertEqual(rawR.getFilter().getName(), "r")
        rawI = self.butler.get("raw", instrument="HSC", exposure=903986, detector=16)
        self.assertEqual(rawI.getFilter().getName(), "i")

    def testCuratedCalibrations(self):
        """Test that defects, the camera, and the brighter-fatter kernel were
        added to the Gen3 registry.
        """
        rawDatasetType = self.butler.registry.getDatasetType("raw")
        butler = Butler(REPO_ROOT, run="calib")
        rawQueryBuilder = SingleDatasetQueryBuilder.fromSingleCollection(
            self.butler.registry,
            rawDatasetType,
            collection="raw"
        )
        cameraRef = None
        bfKernelRef = None
        rawRefs = list(rawQueryBuilder.execute(expandDataId=True))
        for rawRef in rawRefs:
            # Expand raw data ID to include implied dimensions (e.g.
            # physical_filter from exposure).
            rawDataId = DataId(rawRef.dataId, dimensions=rawDatasetType.dimensions.implied(only=False))
            for calibDatasetTypeName in ("camera", "bfKernel", "defects"):
                calibDatasetType = self.butler.registry.getDatasetType(calibDatasetTypeName)
                calibQueryBuilder = SingleDatasetQueryBuilder.fromSingleCollection(
                    self.butler.registry,
                    calibDatasetType,
                    collection="calib"
                )
                calibQueryBuilder.relateDimensions(rawDataId.dimensions())
                calibQueryBuilder.whereDataId(rawDataId)
                calibRefs = list(calibQueryBuilder.execute())
                # We should have exactly one calib of each type for each raw.
                self.assertEqual(len(calibRefs), 1)

                # Try getting those calibs to make sure the files themselves
                # are where the Butler thinks they are.
                # We defer that for camera and bfKernel, because there's only
                # one of each of those.
                if calibDatasetTypeName == "camera":
                    if cameraRef is None:
                        cameraRef = calibRefs[0]
                    else:
                        self.assertEqual(cameraRef, calibRefs[0])
                elif calibDatasetTypeName == "bfKernel":
                    if bfKernelRef is None:
                        bfKernelRef = calibRefs[0]
                    else:
                        self.assertEqual(bfKernelRef, calibRefs[0])
                else:
                    defects = butler.get(calibRefs[0])
                    self.assertIsInstance(defects, lsst.meas.algorithms.Defects)

        instrument = HyperSuprimeCam()
        cameraFromButler = butler.get(cameraRef)
        cameraFromInstrument = instrument.getCamera()
        self.assertEqual(len(cameraFromButler), len(cameraFromInstrument))
        self.assertEqual(cameraFromButler.getName(), cameraFromInstrument.getName())
        self.assertFloatsEqual(butler.get(bfKernelRef), instrument.getBrighterFatterKernel())

    def testBrightObjectMasks(self):
        """Test that bright object masks are included in the Gen3 repo.
        """
        regions = self.butler.get("brightObjectMask", skymap='ci_hsc', tract=0, patch=69,
                                  abstract_filter='r')
        self.assertIsInstance(regions, ObjectMaskCatalog)
        self.assertGreater(len(regions), 0)


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
