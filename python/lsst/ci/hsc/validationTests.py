from lsst.ci.hsc.validate import *

'''
This is where tests to be run during validation should be added. They should be duck typed as a method
instance which takes self, and one argument, the catalog to check. An additional argument should be passed
named extra and should be included if needed but should have a default of None set.
(see the checkApertureCorrections plugin)

To use the extras argument, the validateSources method of a Validate subclass needs to be redeclared,
such that it passes more than one argument to plugins. The extra argument gives the flexibility to 
catch any arguments that may be needed (say compare single and forced measurements) but insure the same
tests can be run in many scenarios. Imagine if ForcedValidation had validateSources redefined to pass
catalog as well as refCatalog to each of the plugins. In this case checkApertureCorrections plugin 
could still be run as catalog is passed in, and the extra variable would catch reh refCatalog, but not
use it. If more than one additional variable is used they can be passed as a list or dict to the plugin
though the extra argument.

Once a plugin (method) is created, it needs to be registered with which type of data it will be used to
validate using the metaRegisterSourcesPlugin decorator. This decorator takes a list of strings which are
the names of the validation subclasses (ie types of processing) to add the test to. Even if the test is
to be added to only one validation class, it must be in a list i.e. ['SfmValidation']
'''

# Register checkApertureCorrections to run in SfmValidation, MeasureValidation, and ForcedValidation
@metaRegisterSourcePlugin(["SfmValidation", "MeasureValidation", "ForcedValidation"])
def checkApertureCorrections(self, catalog, extra=None):
    """Utility function for derived classes that want to verify that aperture corrections were applied
    """
    for alg in ("base_PsfFlux", "base_GaussianFlux"):
        self.assertTrue("Aperture correction fields for %s are present." % alg,
                        (("%s_apCorr" % alg) in catalog.schema) and 
                        (("%s_apCorrSigma" % alg) in catalog.schema) and 
                        (("%s_flag_apCorr" % alg) in catalog.schema))


@metaRegisterSourcePlugin(['MeasureValidation'])
def checkPSF(self, catalog, extra=None):
    self.assertTrue("calib_psfCandidate field exists in deepCoadd_meas catalog",
                    "calib_psfCandidate" in catalog.schema)
    self.assertTrue("calib_psfUsed field exists in deepCoadd_meas catalog",
                    "calib_psfUsed" in catalog.schema)


@metaRegisterSourcePlugin(['MeasureValidation'])
def checkPSFStars(self, catalog, extra=None):
    # Check that at least 95% of the stars we used to model the PSF end up classified as stars
    # on the coadd.  We certainly need much more purity than that to build good PSF models, but
    # this should verify that flag propagation, aperture correction, and extendendess are all
    # running and configured reasonably (but it may not be sensitive enough to detect subtle
    # bugs).
    psfStars = catalog.get("calib_psfUsed")
    extStars = catalog.get("base_ClassificationExtendedness_value") < 0.5 
    self.assertGreater(
        "95% of sources used to build the PSF are classified as stars on the coadd",
        numpy.logical_and(extStars, psfStars).sum(),
        0.95*psfStars.sum()
    )   
