# -*- python -*-

import os
from lsst.pipe.base import Struct
from lsst.sconsUtils.utils import libraryLoaderEnvironment
from lsst.sconsUtils.scripts import BasicSConstruct
from lsst.utils import getPackageDir
from lsst.ci.hsc.validate import RawValidation, DetrendValidation

from SCons.Script import (GetOption, Touch, Execute,
                          AddOption, Mkdir, Dir, Default)

env = BasicSConstruct("ci_hsc", subDirList=["DATA", "python", "bin", "ups"],
                      noCfgFile=True, disableCc=True, followLinks=True)

env["ENV"].update(**os.environ)
env["ENV"]["OMP_NUM_THREADS"] = "1"  # Disable threading; we're parallelising at a higher level


def validate(cls, root, dataId=None, **kwargs):
    """!Construct a command-line for validation

    @param cls  Validation class to use
    @param root  Data repo root directory
    @param dataId  Data identifier dict (Gen2), or None
    @param kwargs  Additional key/value pairs to add to dataId
    @return Command-line string to run validation
    """
    if dataId:
        dataId = dataId.copy()
        dataId.update(kwargs)
    elif kwargs:
        dataId = kwargs
    cmd = [getExecutable("ci_hsc", "validate.py"), cls.__name__, root, ]
    if dataId:
        cmd += ["--id %s" % (" ".join("%s=%s" % (key, value) for key, value in dataId.items()))]
    return " ".join(cmd)


profileNum = -1


def getProfiling(script):
    """Return python command-line argument string for profiling

    If activated (via the "--enable-profile" command-line argument),
    we write the profile to a filename starting with the provided
    base name and including a sequence number and the script name,
    so its contents can be quickly identified.

    Note that this is python function-level profiling, which won't
    descend into C++ elements of the codebase.

    A basic profile can be printed using python:

        >>> from pstats import Stats
        >>> stats = Stats("profile-123-script.pstats")
        >>> stats.sort_stats("cumulative").print_stats(30)
    """
    base = GetOption("enable_profile")
    if not base:
        return ""
    global profileNum
    profileNum += 1
    if script.endswith(".py"):
        script = script[:script.rfind(".")]
    return "-m cProfile -o %s-%03d-%s.pstats" % (base, profileNum, script)


def getExecutable(package, script):
    """
    Given the name of a package and a script or other executable which lies
    within its `bin` subdirectory, return an appropriate string which can be
    used to set up an appropriate environment and execute the command.

    This includes:
    * Specifying an explict list of paths to be searched by the dynamic linker;
    * Specifying a Python executable to be run (we assume the one on the default ${PATH} is appropriate);
    * Specifying the complete path to the script.
    """
    return "{} python {} {}".format(libraryLoaderEnvironment(),
                                    getProfiling(script),
                                    os.path.join(getPackageDir(package), "bin", script))


Execute(Mkdir(".scons"))


root = Dir('.').srcnode().abspath
AddOption("--raw", default=os.path.join(root, "raw"), help="Path to input raw data")
AddOption("--repo", default=os.path.join(root, "DATA"), help="Path for output data repository")
AddOption("--calib", default=os.path.join(root, "CALIB"), help="Path for input calib repository")
AddOption("--no-versions", dest="no_versions", default=False, action="store_true",
          help="Add --no-versions for LSST scripts")
AddOption("--enable-profile", nargs="?", const="profile", dest="enable_profile",
          help=("Profile base filename; output will be <basename>-<sequence#>-<script>.pstats; "
                "(Note: this option is for profiling the scripts, while --profile is for scons)"))

RAW = GetOption("raw")
REPO = GetOption("repo")
CALIB = GetOption("calib")
STDARGS = "--doraise" + (" --no-versions" if GetOption("no_versions") else "")


def command(target, source, cmd):
    """Run a command and record that we ran it

    The record is in the form of a file in the ".scons" directory.
    """
    name = os.path.join(".scons", target)
    if isinstance(cmd, str):
        cmd = [cmd]
    out = env.Command(name, source, cmd + [Touch(name)])
    env.Alias(target, name)
    return out


class Data(Struct):
    """Data we can process"""
    def __init__(self, visit, ccd):
        Struct.__init__(self, visit=visit, ccd=ccd)

    @property
    def name(self):
        """Returns a suitable name for this data"""
        return "%d-%d" % (self.visit, self.ccd)

    @property
    def dataId(self):
        """Returns the dataId for this data"""
        return dict(visit=self.visit, ccd=self.ccd)

    def id(self, prefix="--id", tract=None):
        """Returns a suitable --id command-line string"""
        r = "%s visit=%d ccd=%d" % (prefix, self.visit, self.ccd)
        if tract is not None:
            r += " tract=%d" % tract
        return r


allData = {"HSC-R": [Data(903334, 16),
                     Data(903334, 22),
                     Data(903334, 23),
                     Data(903334, 100),
                     Data(903336, 17),
                     Data(903336, 24),
                     Data(903338, 18),
                     Data(903338, 25),
                     Data(903342, 4),
                     Data(903342, 10),
                     Data(903342, 100),
                     Data(903344, 0),
                     Data(903344, 5),
                     Data(903344, 11),
                     Data(903346, 1),
                     Data(903346, 6),
                     Data(903346, 12),
                     ],
           "HSC-I": [Data(903986, 16),
                     Data(903986, 22),
                     Data(903986, 23),
                     Data(903986, 100),
                     Data(904014, 1),
                     Data(904014, 6),
                     Data(904014, 12),
                     Data(903990, 18),
                     Data(903990, 25),
                     Data(904010, 4),
                     Data(904010, 10),
                     Data(904010, 100),
                     Data(903988, 16),
                     Data(903988, 17),
                     Data(903988, 23),
                     Data(903988, 24),
                     ],
           }

# Set up the data repository
mapper = env.Command(os.path.join(REPO, "_mapper"), ["bin"],
                     ["mkdir -p " + REPO,
                      "echo lsst.obs.hsc.HscMapper > " + os.path.join(REPO, "_mapper"),
                      ])

calib = env.Command(os.path.join(REPO, "CALIB"), mapper,
                    ["rm -f " + os.path.join(REPO, "CALIB"),
                     "ln -s " + CALIB + " " + os.path.join(REPO, "CALIB")]
                    )
ingest = env.Command(os.path.join(REPO, "registry.sqlite3"), calib,
                     [getExecutable("pipe_tasks", "ingestImages.py") + " " + REPO + " " + RAW +
                     "/*.fits --mode=link " + "-c clobber=True register.ignore=True " + STDARGS]
                     )
ingestValidations = [command("ingestValidation-%(visit)d-%(ccd)d" % data.dataId, ingest,
                             validate(RawValidation, REPO, data.dataId)) for
                     data in sum(allData.values(), [])]
calibValidations = [command("calibValidation-%(visit)d-%(ccd)d" % data.dataId, ingest,
                            validate(DetrendValidation, REPO, data.dataId)) for
                    data in sum(allData.values(), [])]

refcatName = "ps1_pv3_3pi_20170110"
refcatPath = os.path.join(REPO, "ref_cats", refcatName)
refcat = env.Command(refcatPath, mapper,
                     ["rm -f " + refcatPath,  # Delete any existing, perhaps leftover from previous
                      "ln -s %s %s" % (os.path.join(root, refcatName), refcatPath)])

brightObjSource = os.path.join(root, "brightObjectMasks")
brightObjTarget = os.path.join(REPO, "deepCoadd", "BrightObjectMasks")
brightObj = env.Command(brightObjTarget, mapper,
                        ["rm -f " + brightObjTarget,  # Delete any existing
                         "mkdir -p " + os.path.dirname(brightObjTarget),
                         "ln -s %s %s" % (brightObjSource, brightObjTarget)])

# Add transmission curves to the repository.
transmissionCurvesTarget = os.path.join(REPO, "transmission")
transmissionCurves = env.Command(transmissionCurvesTarget, calib,
                                 [getExecutable("obs_subaru", "installTransmissionCurves.py") + " " + REPO])

everything = [transmissionCurves, brightObj, refcat, calibValidations, ingestValidations]

env.Alias("all", everything)
Default(everything)

env.Clean(everything, [".scons", "DATA"])
