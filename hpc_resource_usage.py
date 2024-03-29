#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The hpc_resource_usage.py utility plots walltime and memory usage of PBS jobs submitted to the compute cluster.
"""

import argparse
import re
import matplotlib.pyplot as plt
from statistics import median
from pathlib import Path


def medmadmax(data=None, meddata=(), maddata=(), maxdata=()):
    if data is None:
        return [], [], []
    if len(data) == 0:
        data = [0]              # We don't want to raise a median error, just append zeros instead
    meddata.append(median(data))
    maddata.append(1.4826 * median([abs(val - meddata[-1]) for val in data]))     # Robust stdev using the median absolute deviation (MAD) estimator
    maxdata.append(max(data))

    return meddata, maddata, maxdata


def main(datasets: list, maxtime_: float, maxmem_: float, bins: int, summary: bool):

    # Parse the walltime and memory usage
    medtime, madtime, maxtime = medmadmax()
    medmem,  madmem,  maxmem  = medmadmax()
    time                      = dict()
    mem                       = dict()
    for dataset in datasets:
        print(f'Reading logfiles from: "{dataset}"')
        time[dataset] = list()
        mem[dataset]  = list()
        for logfile in [item for item in dataset.parent.glob(dataset.name) if item.is_file()]:
            with open(logfile, 'r') as fid_log:
                try:
                    resources = re.search(r'(Used resources:.*,walltime=.*,mem=.*)\n', fid_log.read())[1].split(',')    # Used resources:	   cput=03:22:23,walltime=01:01:53,mem=17452716032b
                    hhmmss    = resources[1].split('=')[1].split(':')
                    time[dataset].append(float(hhmmss[0]) + float(hhmmss[1])/60 + float(hhmmss[2])/(60*60))
                    mem[dataset].append(float(resources[2].split('=')[1][:-1]) / (1024**3))
                except:
                    print(f"Could not parse: {logfile}")
                    continue
        medtime, madtime, maxtime = medmadmax(time[dataset], medtime, madtime, maxtime)
        medmem,  madmem,  maxmem  = medmadmax(mem[dataset],  medmem,  madmem,  maxmem)
    if all(not time for time in medtime):
        print('Could not find or parse any logfile')
        return

    # Plot the datasets
    fig, axs = plt.subplots(len(datasets) + summary, 2, sharex='col', num='HPC resource usage')
    if len(datasets)==1 and not summary:
        axs = axs.reshape(1, 2)
    for n, dataset in enumerate(datasets):
        axs[n,0].hist(time[dataset], bins=bins, range=(0, min(maxtime_, max(maxtime))))
        axs[n,1].hist( mem[dataset], bins=bins, range=(0, min(maxmem_,  max(maxmem))))
        axs[n,1].text(0.98, 0.94, f"N={len(time[dataset])}", horizontalalignment='right', verticalalignment='top', transform=axs[n,1].transAxes)
        axs[n,0].set_ylabel(dataset.parent.name)
    axs[-1,0].set_xlabel('Walltime (hour)')
    axs[-1,1].set_xlabel('Memory (Gb)')
    for ax in fig.get_axes():
        ax.tick_params(axis='y', left=False, which='both', labelleft=False)

    # Plot the summary
    if summary:
        axs[-1,0].errorbar(medtime, range(len(medtime),0,-1), xerr=[madtime, [a-b for a,b in zip(maxtime,medtime)]], fmt='o')
        axs[-1,1].errorbar(medmem,  range(len(medmem), 0,-1), xerr=[madmem,  [a-b for a,b in zip(maxmem, medmem)]],  fmt='o')
        axs[-1,0].set_ylabel('Summary')
        axs[-1,0].set_xlim(   0, min(maxtime_, max(maxtime)))
        axs[-1,1].set_xlim(   0, min(maxmem_,  max(maxmem)))
        axs[-1,0].set_ylim(-0.5, 1.5+len(medtime))
        axs[-1,1].set_ylim(-0.5, 1.5+len(medmem))

    # plt.tight_layout()
    plt.subplots_adjust(left=0.06, right=0.96, top=0.96)
    plt.show()


if __name__ == '__main__':

    # Parse the input arguments and run main(args)
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description=__doc__)
    parser.add_argument('-w','--walltime', help='Maximum amount of used walltime (in hour) that is shown in the plots', type=float, default=float('Inf'))
    parser.add_argument('-m','--mem',      help='Maximum amount of used memory (in Gb) that is shown in the plots', type=float, default=float('Inf'))
    parser.add_argument('-b','--bins',     help='Number of bins that are shown in the plots', type=int, default=75)
    parser.add_argument('-s','--summary',  help='Show a median summary plot in the final row (left-error = median-absolute-deviation (MAD), right-error = maximum)', action='store_true')
    parser.add_argument('datafolders',     help='Space separated list of folders containing PBS-logfiles. You can append glob-style wildcards for the filenames, otherwise "/*.[oO][0-9uU]*" is appended automatically. It is assumed that the logfiles contain a line similar to "Used resources:	   cput=03:22:23,walltime=01:01:53,mem=17452716032b". Each folder is plotted as a separate row (indicated by the foldername). Try "demo" for plotting fmriprep demo data', nargs='*', default='.')
    args = parser.parse_args()

    if args.datafolders == ['demo']:
        datasets = [Path(__file__).parent/datafolder/'fmriprep_sub-*.o*' for datafolder in ['nthreads=1', 'nthreads=2', 'nthreads=3', 'nthreads=4', 'nthreads=8']]
    else:
        datasets = [Path(datafolder)/'*.[oO][0-9uU]*' if ('?' not in datafolder and '*' not in datafolder and '[' not in datafolder) else Path(datafolder) for datafolder in args.datafolders]

    main(datasets=datasets, maxtime_=args.walltime, maxmem_=args.mem, bins=args.bins, summary=args.summary)
