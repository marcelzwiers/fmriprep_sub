#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fmriprep_sub.py is a wrapper around fmriprep that queries the bids directory for new
participants and then runs participant-level fmriprep jobs on the compute cluster.
"""

import os
import shutil
import glob
import subprocess
import uuid


def main(bidsdir, outputdir, workdir_, outputspace, subject_label=(), force=False, mem_mb=18000, argstr='', dryrun=False, skip=True):

    # Default
    if not outputdir:
        outputdir = os.path.join(bidsdir,'derivatives')

    # Map the bids sub-directories
    if not subject_label:
        sub_dirs = glob.glob(os.path.join(bidsdir, 'sub-*'))
    else:
        sub_dirs = [os.path.join(bidsdir, 'sub-' + label.replace('sub-','')) for label in subject_label]

    # Loop over the bids sub-directories and submit a job for every (new) subject
    for n, sub_dir in enumerate(sub_dirs):

        if not os.path.isdir(sub_dir):
            print('>>> Directory does not exist: ' + sub_dir)
            continue

        sub_id = sub_dir.rsplit('sub-')[1].split(os.sep)[0]

        # A subject is considered already done if there is a html-report. TODO: catch errors
        report = os.path.join(outputdir, 'fmriprep', 'sub-' + sub_id + '.html')
        if not workdir_:
            workdir = os.path.join(os.sep, 'tmp', os.environ['USER'], 'work_fmriprep', f'sub-{sub_id}_{uuid.uuid4()}')
            cleanup = 'rm -rf ' + workdir
        else:
            workdir = os.path.join(workdir_, 'sub-' + sub_id)
            cleanup = ''
        if force or not os.path.isfile(report):

            # Submit the mriqc jobs to the cluster
            # usage: fmriprep [-h] [--version]
            #                 [--participant_label PARTICIPANT_LABEL [PARTICIPANT_LABEL ...]]
            #                 [-t TASK_ID] [--debug] [--nthreads NTHREADS]
            #                 [--omp-nthreads OMP_NTHREADS] [--mem_mb MEM_MB] [--low-mem]
            #                 [--use-plugin USE_PLUGIN] [--anat-only] [--boilerplate]
            #                 [--ignore-aroma-denoising-errors] [-v]
            #                 [--ignore {fieldmaps,slicetiming,sbref} [{fieldmaps,slicetiming,sbref} ...]]
            #                 [--longitudinal] [--t2s-coreg] [--bold2t1w-dof {6,9,12}]
            #                 [--output-space {T1w,template,fsnative,fsaverage,fsaverage6,fsaverage5} [{T1w,template,fsnative,fsaverage,fsaverage6,fsaverage5} ...]]
            #                 [--force-bbr] [--force-no-bbr]
            #                 [--template {MNI152NLin2009cAsym}]
            #                 [--output-grid-reference OUTPUT_GRID_REFERENCE]
            #                 [--template-resampling-grid TEMPLATE_RESAMPLING_GRID]
            #                 [--medial-surface-nan] [--use-aroma]
            #                 [--aroma-melodic-dimensionality AROMA_MELODIC_DIMENSIONALITY]
            #                 [--skull-strip-template {OASIS,NKI}]
            #                 [--skull-strip-fixed-seed] [--fmap-bspline] [--fmap-no-demean]
            #                 [--use-syn-sdc] [--force-syn] [--fs-license-file PATH]
            #                 [--no-submm-recon] [--cifti-output | --fs-no-reconall]
            #                 [-w WORK_DIR] [--resource-monitor] [--reports-only]
            #                 [--run-uuid RUN_UUID] [--write-graph] [--stop-on-first-crash]
            #                 [--notrack]
            #                 bids_dir output_dir {participant}
            pass # Allow for pycharm code folding
            #
            # FMRIPREP: fMRI PREProcessing workflows
            #
            # positional arguments:
            #   bids_dir              the root folder of a BIDS valid dataset (sub-XXXXX folders should be found at the top level in this folder).
            #   output_dir            the output path for the outcomes of preprocessing and visual reports
            #   {participant}         processing stage to be run, only "participant" in the case of FMRIPREP (see BIDS-Apps specification).
            #
            # optional arguments:
            #   -h, --help            show this help message and exit
            #   --version             show program's version number and exit
            #
            # Options for filtering BIDS queries:
            #   --participant_label PARTICIPANT_LABEL [PARTICIPANT_LABEL ...], --participant-label PARTICIPANT_LABEL [PARTICIPANT_LABEL ...]
            #                         a space delimited list of participant identifiers or a single identifier (the sub- prefix can be removed)
            #   -t TASK_ID, --task-id TASK_ID
            #                         select a specific task to be processed
            #
            # Options to handle performance:
            #   --debug               run debug version of workflow
            #   --nthreads NTHREADS, --n_cpus NTHREADS, -n-cpus NTHREADS
            #                         maximum number of threads across all processes
            #   --omp-nthreads OMP_NTHREADS
            #                         maximum number of threads per-process
            #   --mem_mb MEM_MB, --mem-mb MEM_MB
            #                         upper bound memory limit for FMRIPREP processes
            #   --low-mem             attempt to reduce memory usage (will increase disk usage in working directory)
            #   --use-plugin USE_PLUGIN
            #                         nipype plugin configuration file
            #   --anat-only           run anatomical workflows only
            #   --boilerplate         generate boilerplate only
            #   --ignore-aroma-denoising-errors
            #                         ignores the errors ICA_AROMA returns when there are no components classified as either noise or signal
            #   -v, --verbose         increases log verbosity for each occurence, debug level is -vvv
            #
            # Workflow configuration:
            #   --ignore {fieldmaps,slicetiming,sbref} [{fieldmaps,slicetiming,sbref} ...]
            #                         ignore selected aspects of the input dataset to disable corresponding parts of the workflow (a space delimited list)
            #   --longitudinal        treat dataset as longitudinal - may increase runtime
            #   --t2s-coreg           If provided with multi-echo BOLD dataset, create T2*-map and perform T2*-driven coregistration. When multi-echo data
            #                         is provided and this option is not enabled, standard EPI-T1 coregistration is performed using the middle echo.
            #   --bold2t1w-dof {6,9,12}
            #                         Degrees of freedom when registering BOLD to T1w images. 6 degrees (rotation and translation) are used by default.
            #   --output-space {T1w,template,fsnative,fsaverage,fsaverage6,fsaverage5} [{T1w,template,fsnative,fsaverage,fsaverage6,fsaverage5} ...]
            #                         volume and surface spaces to resample functional series into
            #                          - T1w: subject anatomical volume
            #                          - template: normalization target specified by --template
            #                          - fsnative: individual subject surface
            #                          - fsaverage*: FreeSurfer average meshes
            #                         this argument can be single value or a space delimited list,
            #                         for example: --output-space T1w fsnative
            #   --force-bbr           Always use boundary-based registration (no goodness-of-fit checks)
            #   --force-no-bbr        Do not use boundary-based registration (no goodness-of-fit checks)
            #   --template {MNI152NLin2009cAsym}
            #                         volume template space (default: MNI152NLin2009cAsym)
            #   --output-grid-reference OUTPUT_GRID_REFERENCE
            #                         Deprecated after FMRIPREP 1.0.8. Please use --template-resampling-grid instead.
            #   --template-resampling-grid TEMPLATE_RESAMPLING_GRID
            #                         Keyword ("native", "1mm", or "2mm") or path to an existing file. Allows to define a reference grid for the resampling
            #                         of BOLD images in template space. Keyword "native" will use the original BOLD grid as reference. Keywords "1mm" and "2mm"
            #                         will use the corresponding isotropic template resolutions. If a path is given, the grid of that image will be used. It
            #                         determines the field of view and resolution of the output images, but is not used in normalization.
            #   --medial-surface-nan  Replace medial wall values with NaNs on functional GIFTI files. Only performed for GIFTI files mapped to a freesurfer subject (fsaverage or fsnative).
            #
            # Specific options for running ICA_AROMA:
            #   --use-aroma           add ICA_AROMA to your preprocessing stream
            #   --aroma-melodic-dimensionality AROMA_MELODIC_DIMENSIONALITY
            #                         set the dimensionality of MELODIC before runningICA-AROMA
            #
            # Specific options for ANTs registrations:
            #   --skull-strip-template {OASIS,NKI}
            #                         select ANTs skull-stripping template (default: OASIS))
            #   --skull-strip-fixed-seed
            #                         do not use a random seed for skull-stripping - will ensure run-to-run replicability when used with --omp-nthreads 1
            #
            # Specific options for handling fieldmaps:
            #   --fmap-bspline        fit a B-Spline field using least-squares (experimental)
            #   --fmap-no-demean      do not remove median (within mask) from fieldmap
            #
            # Specific options for SyN distortion correction:
            #   --use-syn-sdc         EXPERIMENTAL: Use fieldmap-free distortion correction
            #   --force-syn           EXPERIMENTAL/TEMPORARY: Use SyN correction in addition to fieldmap correction, if available
            #
            # Specific options for FreeSurfer preprocessing:
            #   --fs-license-file PATH
            #                         Path to FreeSurfer license key file. Get it (for free) by registering at https://surfer.nmr.mgh.harvard.edu/registration.html
            #
            # Surface preprocessing options:
            #   --no-submm-recon      disable sub-millimeter (hires) reconstruction
            #   --cifti-output        output BOLD files as CIFTI dtseries
            #   --fs-no-reconall, --no-freesurfer
            #                         disable FreeSurfer surface preprocessing. Note : `--no-freesurfer` is deprecated and will be removed in 1.2. Use `--fs-no-reconall` instead.
            #
            # Other options:
            #   -w WORK_DIR, --work-dir WORK_DIR
            #                         path where intermediate results should be stored
            #   --resource-monitor    enable Nipype's resource monitoring to keep track of memory and CPU usage
            #   --reports-only        only generate reports, don't run workflows. This will only rerun report aggregation, not reportlet generation for specific nodes.
            #   --run-uuid RUN_UUID   Specify UUID of previous run, to include error logs in report. No effect without --reports-only.
            #   --write-graph         Write workflow graph.
            #   --stop-on-first-crash
            #                         Force stopping on first crash, even if a work directory was specified.
            #   --notrack             Opt-out of sending tracking information of this run to the FMRIPREP developers. This information helps to improve FMRIPREP and provides an indicator of real world usage crucial for obtaining funding.

            # Start with a clean directory if we are forcing to reprocess the data (as presumably something went wrong or has changed)
            if force and os.path.isdir(workdir):
                shutil.rmtree(workdir, ignore_errors=True)          # NB: This can also be done in parallel on the cluster if it takes too much time
            if os.path.isfile(report):
                os.remove(report)

            # Submit the job to the compute cluster
            command = """qsub -l walltime=70:00:00,mem={mem_mb}mb -N fmriprep_{sub_id} <<EOF
                         module add fmriprep; cd {pwd}
                         {fmriprep} {bidsdir} {outputdir} participant -w {workdir} --participant-label {sub_id} --output-space {outputspace} --fs-license-file {licensefile} --mem_mb {mem_mb} --omp-nthreads 1 --nthreads 1 {args}
                         {cleanup}\nEOF"""\
                         .format(pwd         = os.getcwd(),
                                 fmriprep    = f'unset PYTHONPATH; singularity run /opt/fmriprep/{os.getenv("FMRIPREP_VERSION")}/fmriprep-{os.getenv("FMRIPREP_VERSION")}.simg',
                                 bidsdir     = bidsdir,
                                 outputdir   = outputdir,
                                 workdir     = workdir,
                                 sub_id      = sub_id,
                                 outputspace = ' '.join(outputspace),
                                 licensefile = os.getenv('FS_LICENSE'),
                                 mem_mb      = mem_mb,
                                 args        = argstr,
                                 cleanup     = cleanup)
            running = subprocess.run('if [ ! -z "$(qselect -s RQH)" ]; then qstat -f $(qselect -s RQH) | grep Job_Name | grep fmriprep_; fi', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            if skip and 'fmriprep_' + sub_id in running.stdout.decode():
                print('>>> Skipping already running / scheduled job: fmriprep_' + sub_id)
            else:
                print(f'>>> Submitting job ({n}/{len(sub_dirs)}):\n{command}')
                if not dryrun:
                    proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                    if proc.returncode != 0:
                        print('WARNING: Job submission failed with error-code {}\n'.format(proc.returncode))

        else:
            print(f'>>> Nothing to do for: {sub_dir} (--> {report})')

    print('\n----------------\n' 
          'Done! Now wait for the jobs to finish... (e.g. check that with this command: qstat $(qselect -s RQ) | grep fmriprep)\n\n'
          'For more details, see:\n\n'
          '  fmriprep -h\n'.format(outputdir=outputdir))


# Shell usage
if __name__ == "__main__":

    # Parse the input arguments and run main(args)
    import argparse
    import textwrap

    class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
        pass

    parser = argparse.ArgumentParser(formatter_class=CustomFormatter, description=textwrap.dedent(__doc__),
                                     epilog='for more information see:\n'
                                            '  module help fmriprep\n'
                                            '  fmriprep -h\n\n'
                                            'examples:\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -o /project/3017065.01/fmriprep --participant_label sub-P010 sub-P018\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -a " --fs-no-reconall"\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -a " --use-aroma --ignore slicetiming"\n'
                                            '  fmriprep_sub.py -f -m 40000 /project/3017065.01/bids -p P018\n\n'
                                            'author:\n'
                                            '  Marcel Zwiers\n ')
    parser.add_argument('bidsdir',                  help='The bids-directory with the (new) subject data')
    parser.add_argument('-o','--outputdir',         help='The output-directory where the frmiprep output is stored (None -> bidsdir/derivatives)')
    parser.add_argument('-w','--workdir',           help='The working-directory where intermediate files are stored (None -> temporary directory')
    parser.add_argument('-p','--participant_label', help='Space seperated list of sub-# identifiers to be processed (the sub- prefix can be removed). Otherwise all sub-folders in the bidsfolder will be processed', nargs='+')
    parser.add_argument('-s','--outputspace',       help='Spatial coordinate system where the functional series are resample into (for more info: fmriprep -h)', default=['template'], choices=['T1w','template','fsnative','fsaverage','fsaverage6','fsaverage5'], nargs='+')
    parser.add_argument('-f','--force',             help='If this flag is given subjects will be processed, regardless of existing folders in the bidsfolder. Otherwise existing folders will be skipped', action='store_true')
    parser.add_argument('-i','--ignore',            help='If this flag is given then already running or scheduled jobs with the same name are ignored, otherwise job submission is skipped', action='store_false')
    parser.add_argument('-m','--mem_mb',            help='Maximum required amount of memory', default=18000, type=int)
    parser.add_argument('-a','--args',              help='Additional arguments that are passed to fmriprep (NB: Use quotes to prevent parsing of spaces)', type=str, default='')
    parser.add_argument('-d','--dryrun',            help='Add this flag to just print the fmriprep qsub commands without actually submitting them (useful for debugging)', action='store_true')
    args = parser.parse_args()

    main(bidsdir=args.bidsdir, outputdir=args.outputdir, workdir_=args.workdir, outputspace=args.outputspace, subject_label=args.participant_label, force=args.force, mem_mb=args.mem_mb, argstr=args.args, dryrun=args.dryrun, skip=args.ignore)
