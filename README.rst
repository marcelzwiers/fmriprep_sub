fmriprep_sub:
-------------

fmriprep_sub.py is a wrapper around fmriprep that queries the bids directory for new participants and then runs participant-level fmriprep jobs on the compute cluster.

BIDScoin is a user friendly `open-source <https://github.com/Donders-Institute/bidscoin>`__ python toolkit that converts ("coins") source-level (raw) neuroimaging data-sets to `nifti <https://nifti.nimh.nih.gov/>`__ / `json <https://www.json.org/>`__ / `tsv <https://en.wikipedia.org/wiki/Tab-separated_values>`__ data-sets that are organized following the Brain Imaging Data Structure, a.k.a. the `BIDS <http://bids.neuroimaging.io>`__ standard. Rather then depending on complex or ambiguous programmatic logic for the identification of imaging modalities, BIDScoin uses a direct mapping approach to identify and convert the raw source data into BIDS data. Different runs of source data are identified by reading information from MRI header files (DICOM or PAR/REC; e.g. 'ProtocolName') and the mapping information about how these different runs should be named in BIDS can be specified a priori as well as interactively by the researcher -- bringing in the missing knowledge that often exists only in his or her head!

BIDScoin is developed at the `Donders Institute <https://www.ru.nl/donders/>`__ of the `Radboud University <https://www.ru.nl/english/>`__.

HPC resource usage
------------------

::

    usage: hpc_resource_usage.py [-h] [-w WALLTIME] [-m MEM] [-b BINS]
                                 [datafolders [datafolders ...]]

    Plots walltime and memory usage of jobs submitted to the compute cluster.

    positional arguments:
      datafolders           Space separated list of folders containing "*.o*" PBS-
                            logfiles. It is assumed that the logfiles contain a
                            line similar to "Used resources:
                            cput=03:22:23,walltime=01:01:53,mem=17452716032b".
                            Each folder is plotted as a separate row (indicated by
                            the foldername). Try "demo" for plotting fmriprep demo
                            data (default: .)

    optional arguments:
      -h, --help            show this help message and exit
      -w WALLTIME, --walltime WALLTIME
                            Maximum amount of used walltime (in hour) that is
                            shown in the plots (default: inf)
      -m MEM, --mem MEM     Maximum amount of used memory (in Gb) that is shown in
                            the plots (default: inf)
      -b BINS, --bins BINS  Number of bins that are shown in the plots (default:
                            75)

.. code-block:: console

   $ hpc_resource_usage.py demo

.. image:: ./hpc_resource_usage.png
   :alt: HPC resource usage histograms
