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


def main(bidsdir, outputdir, workdir, subject_label=(), force=False, mem_mb=18000, file_gb=50, argstr='', dryrun=False, skip=True):

    # Default
    if not outputdir:
        outputdir = os.path.join(bidsdir,'derivatives')

    # Map the bids sub-directories
    if not subject_label:
        sub_dirs = glob.glob(os.path.join(bidsdir, 'sub-*'))
    else:
        sub_dirs = [os.path.join(bidsdir, 'sub-' + label.replace('sub-','')) for label in subject_label]

    # Loop over the bids sub-directories and submit a job for every (new) subject
    for n, sub_dir in enumerate(sub_dirs,1):

        if not os.path.isdir(sub_dir):
            print('>>> Directory does not exist: ' + sub_dir)
            continue

        # Identify what data sessions we have
        sub_id       = 'sub-' + sub_dir.rsplit('sub-')[1].split(os.sep)[0]
        ses_dirs_in  = [os.path.basename(ses_dir) for ses_dir in glob.glob(os.path.join(sub_dir, 'ses-*'))]
        ses_dirs_out = [os.path.basename(ses_dir) for ses_dir in glob.glob(os.path.join(outputdir, 'fmriprep', sub_id, 'ses-*'))]

        # Define a (clean) subject specific work directory and clean-up when done
        if not workdir:
            workdir = os.path.join(os.sep, 'data', os.environ['USER'], '\$\{PBS_JOBID\}')
        else:
            workdir = os.path.join(workdir, sub_id)
            file_gb = 1                                                 # We don't need much local scratch space

        # A subject is considered already done if there is a html-report and all sessions have been processed
        report = os.path.join(outputdir, 'fmriprep', sub_id + '.html')
        if len(ses_dirs_in) == len(ses_dirs_out):
            sessions = [ses_dir_in in ses_dirs_out for ses_dir_in in ses_dirs_in]
        else:
            sessions = [False]
        if force or not os.path.isfile(report) or not all(sessions):

            # Start with a clean directory if we are forcing to reprocess the data (as presumably something went wrong or has changed)
            if not dryrun:
                if force and os.path.isdir(workdir):
                    shutil.rmtree(workdir, ignore_errors=True)          # NB: This can also be done in parallel on the cluster if it takes too much time
                if os.path.isfile(report):
                    os.remove(report)

            # Submit the job to the compute cluster
            command = """qsub -l walltime=70:00:00,mem={mem_mb}mb,file={file_gb}gb,epilogue={epilogue} -N fmriprep_sub-{sub_id} <<EOF
                         module add fmriprep; cd {pwd}
                         {fmriprep} {bidsdir} {outputdir} participant -w {workdir} --participant-label {sub_id} --skip-bids-validation --fs-license-file {licensefile} --mem_mb {mem_mb} --omp-nthreads 1 --nthreads 1 {args}\nEOF"""\
                         .format(pwd         = os.getcwd(),
                                 fmriprep    = f'unset PYTHONPATH; export PYTHONNOUSERSITE=1; singularity run {os.getenv("DCCN_OPT_DIR")}/fmriprep/{os.getenv("FMRIPREP_VERSION")}/fmriprep-{os.getenv("FMRIPREP_VERSION")}.simg',
                                 bidsdir     = bidsdir,
                                 outputdir   = outputdir,
                                 workdir     = workdir,
                                 sub_id      = sub_id[4:],
                                 licensefile = os.getenv('FS_LICENSE'),
                                 mem_mb      = mem_mb,
                                 file_gb     = file_gb,
                                 args        = argstr,
                                 epilogue    = f'{os.getenv("DCCN_OPT_DIR")}/fmriprep/epilogue.sh')
            running = subprocess.run('if [ ! -z "$(qselect -s RQH)" ]; then qstat -f $(qselect -s RQH) | grep Job_Name | grep fmriprep_; fi', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            if skip and f'fmriprep_{sub_id}' in running.stdout.decode():
                print(f'>>> Skipping already running / scheduled job ({n}/{len(sub_dirs)}): fmriprep_{sub_id}')
            else:
                print(f'>>> Submitting job ({n}/{len(sub_dirs)}):\n{command}')
                if not dryrun:
                    proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                    if proc.returncode != 0:
                        print('WARNING: Job submission failed with error-code {}\n'.format(proc.returncode))

        else:
            print(f'>>> Nothing to do for job ({n}/{len(sub_dirs)}): {sub_dir} (--> {report})')

    print('\n----------------\n' 
          'Done! Now wait for the jobs to finish... Check that e.g. with this command:\n\n  qstat -a $(qselect -s RQ) | grep fmriprep_sub\n\n'
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
                                            '  fmriprep_sub.py /project/3017065.01/bids -a " --output-space template" (use for v1.2, deprecated in v1.4)\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -w /project/3017065.01/fmriprep_work\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -o /project/3017065.01/fmriprep --participant_label sub-P010 sub-P018\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -a " --fs-no-reconall"\n'
                                            '  fmriprep_sub.py /project/3017065.01/bids -a " --use-aroma --ignore slicetiming --output-spaces MNI152NLin6Asym"\n'
                                            '  fmriprep_sub.py -f -m 40000 /project/3017065.01/bids -p P018\n\n'
                                            'author:\n'
                                            '  Marcel Zwiers\n ')
    parser.add_argument('bidsdir',                  help='The bids-directory with the (new) subject data')
    parser.add_argument('-o','--outputdir',         help='The output-directory where the frmiprep output is stored (None -> bidsdir/derivatives)')
    parser.add_argument('-w','--workdir',           help='The working-directory where intermediate files are stored (None -> temporary directory')
    parser.add_argument('-p','--participant_label', help='Space seperated list of sub-# identifiers to be processed (the sub- prefix can be removed). Otherwise all sub-folders in the bidsfolder will be processed', nargs='+')
    parser.add_argument('-f','--force',             help='If this flag is given subjects will be processed, regardless of existing folders in the bidsfolder. Otherwise existing folders will be skipped', action='store_true')
    parser.add_argument('-i','--ignore',            help='If this flag is given then already running or scheduled jobs with the same name are ignored, otherwise job submission is skipped', action='store_false')
    parser.add_argument('-m','--mem_mb',            help='Maximum required amount of memory (in mb)', default=18000, type=int)
    parser.add_argument('-s','--scratch_gb',        help='Maximum required free diskspace of the workdir (in gb)', default=50, type=int)
    parser.add_argument('-a','--args',              help='Additional arguments that are passed to fmriprep (NB: Use quotes and a leading space to prevent unintended parsing)', type=str, default='')
    parser.add_argument('-d','--dryrun',            help='Add this flag to just print the fmriprep qsub commands without actually submitting them (useful for debugging)', action='store_true')
    args = parser.parse_args()

    main(bidsdir=args.bidsdir, outputdir=args.outputdir, workdir=args.workdir, subject_label=args.participant_label, force=args.force, mem_mb=args.mem_mb, file_gb=args.scratch_gb, argstr=args.args, dryrun=args.dryrun, skip=args.ignore)
