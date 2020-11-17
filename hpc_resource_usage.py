#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plots walltime and memory usage of jobs submitted to the compute cluster.
"""

import argparse
import re
import matplotlib.pyplot as plt
from pathlib import Path


def main(datadirs: list, maxwalltime_: float, maxmem_: float, bins: int):

    # Parse the walltime and memory usage
    maxwalltime = 0
    maxmem      = 0
    walltime    = dict()
    mem         = dict()
    for datadir in datadirs:
        print(f'Reading logfiles from: "{datadir}"')
        walltime[datadir] = list()
        mem[datadir]      = list()
        for logfile in [item for item in datadir.glob('*.o*') if item.is_file()]:
            with open(logfile, 'r') as fid_log:
                try:
                    resources = re.search(r'(Used resources:.*,walltime=.*,mem=.*)\n', fid_log.read())[1].split(',')    # Used resources:	   cput=03:22:23,walltime=01:01:53,mem=17452716032b
                    hhmmss    = resources[1].split('=')[1].split(':')
                    walltime[datadir].append(float(hhmmss[0]) + float(hhmmss[1])/60 + float(hhmmss[2])/(60*60))
                    mem[datadir].append(float(resources[2].split('=')[1][:-1]) / (1024**3))
                except:
                    print(f"Could not parse: {logfile}")
                    continue
        maxwalltime = max([maxwalltime] + walltime[datadir])
        maxmem      = max([maxmem] + mem[datadir])
    if maxwalltime==0 and maxmem==0:
        print('Could not find or parse any logfile')
        return

    # Plot the data
    fig, axs = plt.subplots(len(datadirs), 2)
    if len(datadirs) == 1:
        axs = axs.reshape(1, 2)
    for n, datadir in enumerate(datadirs):
        axs[n,0].hist(walltime[datadir], bins=bins, range=(0,min(maxwalltime,maxwalltime_)))
        axs[n,1].hist(     mem[datadir], bins=bins, range=(0,min(maxmem,maxmem_)))
        axs[n,0].set_ylabel(datadir.name)
    axs[-1,0].set_xlabel('Walltime (hour)')
    axs[-1,1].set_xlabel('Memory (Gb)')
    for ax in fig.get_axes():
        ax.tick_params(axis='y', left=False, which='both', labelleft=False)
        ax.label_outer()

    plt.tight_layout()
    plt.show()


if __name__ == '__main__':

    # Parse the input arguments and run hpc_resource_usage(args)
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description=__doc__)
    parser.add_argument('-w','--walltime', help='Maximum amount of used walltime (in hour) that is shown in the plots', type=float, default=float('Inf'))
    parser.add_argument('-m','--mem',      help='Maximum amount of used memory (in Gb) that is shown in the plots', type=float, default=float('Inf'))
    parser.add_argument('-b','--bins',     help='Number of bins that are shown in the plots', type=int, default=75)
    parser.add_argument('datafolders',     help='Space separated list of folders containing "*.o*" PBS-logfiles. It is assumed that the logfiles contain a line similar to "Used resources:	   cput=03:22:23,walltime=01:01:53,mem=17452716032b". Each folder is plotted as a separate row (indicated by the foldername). Try "demo" for plotting fmriprep demo data', nargs='*', default='.')
    args = parser.parse_args()

    if args.datafolders == ['demo']:
        args.datafolders = ['/opt/fmriprep/dccn/nthreads = 1', '/opt/fmriprep/dccn/nthreads = 2', '/opt/fmriprep/dccn/nthreads = 4', '/opt/fmriprep/dccn/nthreads = 8']

    main(datadirs=[Path(datadir) for datadir in args.datafolders], maxwalltime_=args.walltime, maxmem_=args.mem, bins=args.bins)
