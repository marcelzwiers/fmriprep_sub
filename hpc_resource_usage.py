#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The hpc_resource_usage.py utility plots walltime and memory usage of PBS jobs submitted to the compute cluster.
"""

import argparse
import re
import matplotlib.pyplot as plt
from statistics import mean, stdev
from pathlib import Path


def meanstdmax(data=None, meandata=(), stddata=(), maxdata=()):
    if data is None:
        return [], [], []
    if len(data) == 0:
        data = [0,0]            # We don't want to raise a stdev error, just append zeros instead
    if len(data) == 1:
        data = data + data      # We don't want to raise a stdev error, just append (data, 0, data) instead
    meandata.append(mean(data))
    stddata.append(stdev(data))
    maxdata.append(max(data))

    return meandata, stddata, maxdata


def main(datadirs: list, maxwalltime_: float, maxmem_: float, bins: int, summary: bool):

    # Parse the walltime and memory usage
    meanwalltime, stdwalltime, maxwalltime = meanstdmax()
    meanmem, stdmem, maxmem                = meanstdmax()
    walltime                               = dict()
    mem                                    = dict()
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
        meanwalltime, stdwalltime, maxwalltime = meanstdmax(walltime[datadir], meanwalltime, stdwalltime, maxwalltime)
        meanmem,      stdmem,      maxmem      = meanstdmax(mem[datadir],      meanmem,      stdmem,      maxmem)
    if all(not time for time in meanwalltime):
        print('Could not find or parse any logfile')
        return

    # Plot the data
    fig, axs = plt.subplots(len(datadirs) + summary, 2, sharex='col', num='HPC resource usage')
    if len(datadirs)==1 and not summary:
        axs = axs.reshape(1, 2)
    for n, datadir in enumerate(datadirs):
        axs[n,0].hist(walltime[datadir], bins=bins, range=(0, min(maxwalltime_,max(maxwalltime))))
        axs[n,1].hist(     mem[datadir], bins=bins, range=(0, min(maxmem_,max(maxmem))))
        axs[n,1].text(0.98, 0.94, f"N={len(walltime[datadir])}", horizontalalignment='right', verticalalignment='top', transform=axs[n,1].transAxes)
        axs[n,0].set_ylabel(datadir.name)
    axs[-1,0].set_xlabel('Walltime (hour)')
    axs[-1,1].set_xlabel('Memory (Gb)')
    for ax in fig.get_axes():
        ax.tick_params(axis='y', left=False, which='both', labelleft=False)

    # Plot the summary data
    if summary:
        axs[-1,0].errorbar(meanwalltime, range(len(meanwalltime),0,-1), xerr=[stdwalltime, [a-b for a,b in zip(maxwalltime,meanwalltime)]], fmt='o')
        axs[-1,1].errorbar(meanmem,      range(len(meanmem),0,-1),      xerr=[stdmem,      [a-b for a,b in zip(maxmem,     meanmem)]],      fmt='o')
        axs[-1,0].set_ylabel('Summary')
        axs[-1,0].set_ylim(-0.5, 1.5+len(meanwalltime))
        axs[-1,1].set_ylim(-0.5, 1.5+len(meanmem))

    # plt.tight_layout()
    plt.show()


if __name__ == '__main__':

    # Parse the input arguments and run hpc_resource_usage(args)
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description=__doc__)
    parser.add_argument('-w','--walltime', help='Maximum amount of used walltime (in hour) that is shown in the plots', type=float, default=float('Inf'))
    parser.add_argument('-m','--mem',      help='Maximum amount of used memory (in Gb) that is shown in the plots', type=float, default=float('Inf'))
    parser.add_argument('-b','--bins',     help='Number of bins that are shown in the plots', type=int, default=75)
    parser.add_argument('-s','--summary',  help='Show a summary plot in the final row (left-error = stdev, right-error = max)', action='store_true')
    parser.add_argument('datafolders',     help='Space separated list of folders containing "*.o*" PBS-logfiles. It is assumed that the logfiles contain a line similar to "Used resources:	   cput=03:22:23,walltime=01:01:53,mem=17452716032b". Each folder is plotted as a separate row (indicated by the foldername). Try "demo" for plotting fmriprep demo data', nargs='*', default='.')
    args = parser.parse_args()

    if args.datafolders == ['demo']:
        args.datafolders = [Path(__file__).parent/'nthreads=1', Path(__file__).parent/'nthreads=2', Path(__file__).parent/'nthreads=3', Path(__file__).parent/'nthreads=4', Path(__file__).parent/'nthreads=8']

    main(datadirs=[Path(datadir) for datadir in args.datafolders], maxwalltime_=args.walltime, maxmem_=args.mem, bins=args.bins, summary=args.summary)
