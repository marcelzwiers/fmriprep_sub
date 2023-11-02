#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The fmriprep_sub.py utility is a wrapper around fmriprep that queries the BIDS directory for new participants and
then runs them (as single-participant fmriprep jobs) on the compute cluster.
"""

import os
import shutil
import subprocess
import argparse
import textwrap
from pathlib import Path

version = os.getenv("FMRIPREP_VERSION")


def main(bidsdir: str, outputdir: str, workroot: str, subject_label=(), force=False, manager='torque', mem_mb=20000, walltime=72, file_gb_=50, nthreads=None, argstr='', qargstr='', dryrun=False, skip=True):

    # Defaults
    bidsdir   = Path(bidsdir)
    outputdir = Path(outputdir)
    if not outputdir.name:
        outputdir = bidsdir/'derivatives'/'fmriprep'
    if not nthreads:                                        # Set the number of threads between 1 and 8 (but see https://github.com/nipreps/fmriprep/pull/2071)
        nthreads = min(8, max(1, round(mem_mb / 10000)))    # Allocating ~10GB / CPU core

    # Map the bids subdirectories
    if not subject_label:
        sub_dirs = list(bidsdir.glob('sub-*'))
    else:
        sub_dirs = [bidsdir/('sub-' + label.replace('sub-','')) for label in subject_label]

    # Loop over the bids subdirectories and submit a job for every (new) subject
    for n, sub_dir in enumerate(sub_dirs, 1):

        if not sub_dir.is_dir():
            print(f">>> Directory does not exist: {sub_dir}")
            continue

        # Identify what data sessions we have
        sub_id       = [part for part in sub_dir.parts if part.startswith('sub-')][0]
        ses_dirs_in  = [ses_dir.name for ses_dir in sub_dir.glob('ses-*')]
        ses_dirs_out = [ses_dir.name for ses_dir in (outputdir/sub_id).glob('ses-*')]

        # Define a (clean) subject specific work directory and allocate space there
        file_gb = ''                # By default, we don't need to allocate local scratch space
        if not workroot:
            workdir = Path('\$TMPDIR')/sub_id
            if manager == 'torque':
                file_gb = f",file={file_gb_}gb"
            elif manager == 'slurm':
                file_gb = f"--tmp={file_gb_}G"
        else:
            workdir = Path(workroot)/sub_id

        # A subject is considered already done if there is a html-report and all sessions have been processed
        report = outputdir/(sub_id + '.html')
        if len(ses_dirs_in) == len(ses_dirs_out):
            sessions = [ses_dir_in in ses_dirs_out for ses_dir_in in ses_dirs_in]
        else:
            sessions = [False]
        if force or not report.is_file() or not all(sessions):

            # Start with a clean directory if we are forcing to reprocess the data (as presumably something went wrong or has changed)
            if not dryrun:
                if force and workdir.is_dir():
                    shutil.rmtree(workdir, ignore_errors=True)          # NB: This can also be done in parallel on the cluster if it takes too much time
                if report.is_file():
                    report.unlink()

            # Generate the submit-command
            if manager == 'torque':
                submit  = f"qsub -l nodes=1:ppn={nthreads},walltime={walltime}:00:00,mem={mem_mb}mb{file_gb} -N fmriprep_{sub_id} {qargstr}"
                running = subprocess.run('if [ ! -z "$(qselect -s RQH)" ]; then qstat -f $(qselect -s RQH) | grep Job_Name | grep fmriprep_sub; fi', shell=True, capture_output=True, text=True)
            elif manager == 'slurm':
                submit  = f"sbatch --job-name=fmriprep_{sub_id} --mem={mem_mb} --time={walltime}:00:00 --ntasks=1 --cpus-per-task={nthreads} {file_gb} {qargstr}"
                running = subprocess.run('squeue -h -o format=%j | grep fmriprep_sub', shell=True, capture_output=True, text=True)
            else:
                print(f"ERROR: Invalid resource manager `{manager}`")
                exit(1)

            # Generate the fmriprep-job
            job = textwrap.dedent('''\
                #!/bin/bash
                {sleep}
                ulimit -v unlimited
                echo using: TMPDIR=\$TMPDIR
                cd {pwd}
                {fmriprep} {bidsdir} {outputdir} participant -w {workdir} --participant-label {sub_id} {validation} --fs-license-file {licensefile} --mem_mb {mem_mb} --omp-nthreads {nthreads} --nthreads {nthreads} {args}'''
                .format(pwd         = Path.cwd(),
                        sleep       = 'sleep 1m' if n>1 else '',                                            # Avoid concurrency issues, see: https://neurostars.org/t/updated-fmriprep-workaround-for-running-subjects-in-parallel/6677
                        fmriprep    = f'apptainer run --cleanenv --bind \$TMPDIR:/tmp {os.getenv("DCCN_OPT_DIR")}/fmriprep/{version}/fmriprep-{version}.simg',
                        bidsdir     = bidsdir,
                        outputdir   = outputdir.parent if int(version.split('.')[0])<21 else outputdir,     # Use legacy or bids output-layout (https://fmriprep.org/en/latest/outputs.html#layout)
                        workdir     = workdir,
                        sub_id      = sub_id[4:],
                        validation  = '--skip-bids-validation' if n>1 else '',
                        licensefile = os.getenv('FS_LICENSE'),
                        nthreads    = nthreads,
                        mem_mb      = mem_mb,
                        args        = argstr))

            # Submit the job to the compute cluster
            command = f"{submit} <<EOF\n{job}\nEOF\n"
            if skip and f'fmriprep_{sub_id}' in running.stdout:
                print(f">>> Skipping already running / scheduled job ({n}/{len(sub_dirs)}): fmriprep_{sub_id}")
            else:
                print(f">>> Submitting job ({n}/{len(sub_dirs)}):\n{command}")
                if not dryrun:
                    process = subprocess.run(command, shell=True, capture_output=True, text=True)
                    if process.returncode != 0:
                        print(f"ERROR {process.returncode}: Job submission failed\n{process.stderr}\n{process.stdout}")

        else:
            print(f">>> Nothing to do for job ({n}/{len(sub_dirs)}): {sub_dir} (--> {report})")

    if not sub_dirs:
        print(f"No BIDS sub-folders found in {bidsdir}")
    elif dryrun:
        print('\n----------------\nDone! NB: The printed jobs were not actually submitted')
    else:
        print('\n----------------\n'
             f"Done! Now wait for the jobs to finish... Check that e.g. with this command:\n\n  {'qstat - a $(qselect -s RQ)' if manager=='torque' else 'squeue -u '+os.getenv('USER')} | grep fmriprep_sub\n\n"
              'You can check how much memory and walltime your jobs have used by running:\n\n  hpc_resource_usage.py\n\n'
              "After your jobs have finished you can create a group report using BIDScoin's slicereport tool. For instance,\n"
              'to inspect mean fMRI images you can load the bidscoin/4.2.2 module (or newer) and run:\n\n'
             f"  slicereport {outputdir} func/*desc-preproc_bold* -x {outputdir}")


# Shell usage
if __name__ == "__main__":

    # Parse the input arguments and run main(args)
    class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
        pass

    parser = argparse.ArgumentParser(formatter_class=CustomFormatter, description=textwrap.dedent(__doc__),
                                     epilog='for more information see:\n'
                                            '  module help fmriprep\n'
                                            '  fmriprep -h\n\n'
                                            'examples:\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -a " --output-space template" (use for v1.2, deprecated in v1.4)\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -w /project/3017065.01/fmriprep_work\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -o /project/3017065.01/fmriprep --participant_label sub-P010 sub-P018\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -a " --fs-no-reconall"\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -a " --use-aroma --return-all-components --ignore slicetiming --output-spaces MNI152NLin6Asym"\n'
                                            '  fmriprep_sub.py -f -m 40000 /project/3017065.01/bids -p P018\n\n'
                                            'author:\n'
                                            '  Marcel Zwiers\n ')
    parser.add_argument('bidsdir',                  help='The bids-directory with the (new) subject data')
    parser.add_argument('-o','--outputdir',         help='The output-directory where the frmiprep output is stored. NB: for fmriprep versions before 21.0.0 the last part of outputdir MUST be named "fmriprep" (e.g. "data/v20.0.0/fmriprep") (default = bidsdir/derivatives/fmriprep)', default='')
    parser.add_argument('-w','--workdir',           help='The working-directory where intermediate files are stored (default = a temporary directory', default='')
    parser.add_argument('-p','--participant_label', help='Space seperated list of sub-# identifiers to be processed (the sub- prefix can be removed). Otherwise all sub-folders in the bidsfolder will be processed', nargs='+')
    parser.add_argument('-f','--force',             help='If this flag is given subjects will be processed, regardless of existing folders in the bidsfolder. Otherwise existing folders will be skipped', action='store_true')
    parser.add_argument('-i','--ignore',            help='If this flag is given then already running or scheduled jobs with the same name are ignored, otherwise job submission is skipped', action='store_false')
    parser.add_argument('-r','--resourcemanager',   help='Resource manager to which the jobs are submitted', choices=('torque', 'slurm'), default='torque', const='torque', nargs='?')
    parser.add_argument('-m','--mem_mb',            help='Required amount of memory (in mb)', default=20000, type=int)
    parser.add_argument('-n','--nthreads',          help='Number of compute threads (CPU cores) per job (subject). By default ~10GB/CPU core is allocated, i.e. nthreads = round(mem_mb/10000), but you can increase it to speed up the processing of small datasets (< ~25 subjects), see https://fmriprep.org/en/stable/faq.html#running-subjects-in-parallel', choices=range(1,9), type=int)
    parser.add_argument('-t','--time',              help='Required walltime (in hours)', default=72, type=int)
    parser.add_argument('-s','--scratch_gb',        help='Required free diskspace of the local temporary workdir (in gb)', default=50, type=int)
    parser.add_argument('-a','--args',              help='Additional arguments that are passed to fmriprep (NB: Use quotes and a leading space to prevent unintended argument parsing)', type=str, default='')
    parser.add_argument('-q','--qargs',             help='Additional arguments that are passed to qsub/sbatch (NB: Use quotes and a leading space to prevent unintended argument parsing)', type=str, default='')
    parser.add_argument('-d','--dryrun',            help='Add this flag to just print the fmriprep qsub commands without actually submitting them (useful for debugging)', action='store_true')
    args = parser.parse_args()

    # Catch old fmriprep outputdir behaviour
    if int(version.split('.')[0]) < 21 and args.outputdir and Path(args.outputdir).name != 'fmriprep':
        print(f"For fmriprep versions before 21.0.0 the last part of outputdir MUST be named 'fmriprep'\n(e.g. '{Path(args.outputdir).parent}/fmriprep' instead of '{args.outputdir}')")

    else:
        main(bidsdir       = args.bidsdir,
             outputdir     = args.outputdir,
             workroot      = args.workdir,
             subject_label = args.participant_label,
             force         = args.force,
             manager       = args.resourcemanager,
             mem_mb        = args.mem_mb,
             walltime      = args.time,
             nthreads      = args.nthreads,
             file_gb_      = args.scratch_gb,
             argstr        = args.args,
             qargstr       = args.qargs,
             dryrun        = args.dryrun,
             skip          = args.ignore)
