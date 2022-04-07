==========
``ci_hsc``
==========

``ci_hsc`` is a simple metapackage for the ci_hsc_gen3
packages, which test most of the LSST Data Release Production
pipelines on a minimal HSC test dataset (the testdata_ci_hsc package),
using different middleware systems for data access and execution.

History
=======

``ci_hsc`` previously included both the test data and the scripts for
(Gen2 middleware) pipeline execution directly.

As such, it remains a `Git LFS`_ repo; even though LFS functionality
is no longer used on the tip of the master branch, it is used in
previous revisions.  See `LSST documentation on Git LFS`_ for more information.

.. _Git LFS: https://git-lfs.github.com
.. _LSST documentation on Git LFS: http://developer.lsst.io/en/latest/tools/git_lfs.html
