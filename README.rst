Utilities for using `fmriprep <https://fmriprep.org>`__ on the compute cluster at the `Donders Institute <https://www.ru.nl/donders/>`__ of the `Radboud University <https://www.ru.nl/english/>`__.

fmriprep_sub:
=============

The ``fmriprep_sub.py`` is a wrapper around fmriprep that queries the `BIDS <http://bids.neuroimaging.io>`__ directory for new participants and then runs participant-level fmriprep jobs on the compute cluster.

Example:
--------

.. code-block:: console

   $ fmriprep_sub.py /project/3017065.01/bids --nthreads 4 --mem_mb 28000

HPC resource usage
==================


Example:
--------

.. code-block:: console

   $ hpc_resource_usage.py demo
   Reading logfiles from: "/opt/fmriprep/dccn/nthreads = 1"
   Reading logfiles from: "/opt/fmriprep/dccn/nthreads = 2"
   Reading logfiles from: "/opt/fmriprep/dccn/nthreads = 4"
   Reading logfiles from: "/opt/fmriprep/dccn/nthreads = 8"

.. image:: ./hpc_resource_usage.png
   :alt: HPC resource usage histograms
