#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FMRIPrep_SUB is a wrapper around fmriprep that queries the bids directory for new
participants and then runs participant-level fmriprep jobs on the compute cluster.
"""


def main(bidsdir, outputdir, sessions=(), force=False, mem_mb=18000, argstr=''):

    import os
    import glob
    import subprocess

    # Default
    if not outputdir:
        outputdir = os.path.join(bidsdir,'derivatives','fmriprep')

    # Scan the bids-directory for new subjects
    if not sessions:
        sessions = glob.glob(os.path.join(bidsdir, 'sub-*'+os.sep+'ses-*'), recursive=True)
    for session in sessions:
        if force or not glob.glob(os.path.join(outputdir,'reports','sub-' + session.rsplit('sub-')[1].replace(os.sep,'_') + '_*.html')):    # TODO: Adapt

            # Submit the mriqc jobs to the cluster
            # usage: mriqc [-h] [--version]
            #              [--participant_label [PARTICIPANT_LABEL [PARTICIPANT_LABEL ...]]]
            #              [--session-id [SESSION_ID [SESSION_ID ...]]]
            #              [--run-id [RUN_ID [RUN_ID ...]]]
            #              [--task-id [TASK_ID [TASK_ID ...]]]
            #              [-m [{T1w,bold,T2w} [{T1w,bold,T2w} ...]]] [--dsname DSNAME]
            #              [-w WORK_DIR] [--verbose-reports] [--write-graph] [--dry-run]
            #              [--profile] [--use-plugin USE_PLUGIN] [--no-sub] [--email EMAIL]
            #              [-v] [--webapi-url WEBAPI_URL] [--webapi-port WEBAPI_PORT]
            #              [--upload-strict] [--n_procs N_PROCS] [--mem_gb MEM_GB]
            #              [--testing] [-f] [--ica] [--hmc-afni] [--hmc-fsl]
            #              [--fft-spikes-detector] [--fd_thres FD_THRES]
            #              [--ants-nthreads ANTS_NTHREADS] [--ants-float]
            #              [--ants-settings ANTS_SETTINGS] [--deoblique] [--despike]
            #              [--start-idx START_IDX] [--stop-idx STOP_IDX]
            #              [--correct-slice-timing]
            #              bids_dir output_dir {participant,group} [{participant,group} ...]
            pass # Allow for pycharm code folding
            # MRIQC: MRI Quality Control
            #
            # positional arguments:
            #   bids_dir              The directory with the input dataset formatted according to the BIDS standard.
            #   output_dir            The directory where the output files should be stored. If you are running group level analysis this folder should be prepopulated with the results of theparticipant level analysis.
            #   {participant,group}   Level of the analysis that will be performed. Multiple participant level analyses can be run independently (in parallel) using the same output_dir.
            #
            # optional arguments:
            #   -h, --help            show this help message and exit
            #   --version             show program's version number and exit
            #
            # Options for filtering the input BIDS dataset:
            #   --participant_label [PARTICIPANT_LABEL [PARTICIPANT_LABEL ...]], --participant-label [PARTICIPANT_LABEL [PARTICIPANT_LABEL ...]]
            #                         one or more participant identifiers (the sub- prefix can be removed)
            #   --session-id [SESSION_ID [SESSION_ID ...]]
            #                         filter input dataset by session id
            #   --run-id [RUN_ID [RUN_ID ...]]
            #                         filter input dataset by run id (only integer run ids are valid)
            #   --task-id [TASK_ID [TASK_ID ...]]
            #                         filter input dataset by task id
            #   -m [{T1w,bold,T2w} [{T1w,bold,T2w} ...]], --modalities [{T1w,bold,T2w} [{T1w,bold,T2w} ...]]
            #                         filter input dataset by MRI type ("T1w", "T2w", or "bold")
            #   --dsname DSNAME       a dataset name
            #
            # Instrumental options:
            #   -w WORK_DIR, --work-dir WORK_DIR
            #                         change the folder to store intermediate results
            #   --verbose-reports
            #   --write-graph         Write workflow graph.
            #   --dry-run             Do not run the workflow.
            #   --profile             hook up the resource profiler callback to nipype
            #   --use-plugin USE_PLUGIN
            #                         nipype plugin configuration file
            #   --no-sub              Turn off submission of anonymized quality metrics to MRIQC's metrics repository.
            #   --email EMAIL         Email address to include with quality metric submission.
            #   -v, --verbose         increases log verbosity for each occurence, debug level is -vvv
            #   --webapi-url WEBAPI_URL
            #                         IP address where the MRIQC WebAPI is listening
            #   --webapi-port WEBAPI_PORT
            #                         port where the MRIQC WebAPI is listening
            #   --upload-strict       upload will fail if if upload is strict
            #
            # Options to handle performance:
            #   --n_procs N_PROCS, --nprocs N_PROCS, --n_cpus N_PROCS, --nprocs N_PROCS
            #                         number of threads
            #   --mem_gb MEM_GB       available total memory
            #   --testing             use testing settings for a minimal footprint
            #   -f, --float32         Cast the input data to float32 if it's represented in higher precision (saves space and improves perfomance)
            #
            # Workflow configuration:
            #   --ica                 Run ICA on the raw data and include the componentsin the individual reports (slow but potentially very insightful)
            #   --hmc-afni            Use ANFI 3dvolreg for head motion correction (HMC) - default
            #   --hmc-fsl             Use FSL MCFLIRT instead of AFNI for head motion correction (HMC)
            #   --fft-spikes-detector
            #                         Turn on FFT based spike detector (slow).
            #   --fd_thres FD_THRES   motion threshold for FD computation
            #
            # Specific settings for ANTs:
            #   --ants-nthreads ANTS_NTHREADS
            #                         number of threads that will be set in ANTs processes
            #   --ants-float          use float number precision on ANTs computations
            #   --ants-settings ANTS_SETTINGS
            #                         path to JSON file with settings for ANTS
            #
            # Specific settings for AFNI:
            #   --deoblique           Deoblique the functional scans during head motion correction preprocessing
            #   --despike             Despike the functional scans during head motion correction preprocessing
            #   --start-idx START_IDX
            #                         Initial volume in functional timeseries that should be considered for preprocessing
            #   --stop-idx STOP_IDX   Final volume in functional timeseries that should be considered for preprocessing
            #   --correct-slice-timing
            #                         Perform slice timing correction
            sub_id  = session.rsplit('sub-')[1].split(os.sep)[0]
            ses_id  = session.rsplit('ses-')[1]
            command = """qsub -l walltime=24:00:00,mem={mem_mb}gb -N fmriprep_{sub_id}_{ses_id} <<EOF
                         module add fmriprep; source activate /opt/fmriprep
                         fmriprep {bidsdir} {outputdir} participant -w {workdir} --participant-label {sub_id} --mem_mb {mem_mb} --ants-nthreads 1 --nthreads 1 {args}\nEOF"""\
                         .format(bidsdir=bidsdir, outputdir=outputdir, workdir=os.path.join(outputdir,'work',sub_id+'_'+ses_id), sub_id=sub_id, ses_id=ses_id, mem_mb=mem_mb, args=argstr)
            print('>>> Submitting job:\n' + command)
            proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            if proc.returncode != 0:
                print('Job submission failed with error-code: {}\n'.format(proc.returncode))

    print('\n----------------\nDone! Now wait for the jobs to finish before running the group-level QC, e.g. like this:\n  fmriprep {bidsdir} {outputdir} group\n\nFor more details, see:\n  fmriprep -h\n'.format(bidsdir=bidsdir, outputdir=outputdir))


# Shell usage
if __name__ == "__main__":

    # Parse the input arguments and run bidscoiner(args)
    import argparse
    import textwrap

    class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
        pass

    parser = argparse.ArgumentParser(formatter_class=CustomFormatter,
                                     description=textwrap.dedent(__doc__),
                                     epilog='examples:\n  mriqc_sub.py /project/3022026.01/bids\n  mriqc_sub.py /project/3022026.01/bids -o /project/3022026.01/fmriprep --sessions sub-010/ses-mri01 sub-011/ses-mri01\n  mriqc_sub.py -f -m 16 /project/3022026.01/bids -s sub-013/ses-mri01\n\nAuthor:\n  Marcel Zwiers\n ')
    parser.add_argument('bidsdir',          help='The bids-directory with the (new) subject data')
    parser.add_argument('-o','--outputdir', help='The output-directory where the frmiprep output is stored (None = ./bidsdir/derivatives/fmriprep)')
    parser.add_argument('-s','--sessions',  help='Space seperated list of selected sub-#/ses-# names / folders to be processed. Otherwise all sessions in the rawfolder will be selected', nargs='*')
    parser.add_argument('-f','--force',     help='If this flag is given subjects will be processed, regardless of existing folders in the bidsfolder. Otherwise existing folders will be skipped', action='store_true')
    parser.add_argument('-m','--mem_gb',    help='Maximum required amount of memory', default=18)
    parser.add_argument('-a','--args',      help='Additional arguments that are passed to fmriprep (NB: Use quotes to prevent parsing of spaces)')
    args = parser.parse_args()

    main(bidsdir=args.bidsdir, outputdir=args.outputdir, sessions=args.sessions, force=args.force, mem_gb=args.mem_gb, argstr=args.args)
