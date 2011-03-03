#!/usr/bin/env python
# from datasets to arrays
# do everything buildimages did
# plus half of what buildworld did

from __future__ import division
import sys
sys.path.append('..')
import os
from argparse import ArgumentParser
from multiprocessing import cpu_count
from dataset import getDataset, checkDataset, listDatasets, dsDict, getDatasetDims
from tile import checkTile, checkStartEnd, processTiles
from bathy import maxdepth, slope, checkMaxdepth, checkSlope
# write checkMaxelev, checkSealevel etc?
from mcarray import maxelev, createArrays, saveArrays
from crust import makeCrustIDT
from ore import placeOre
from building import building

def checkProcesses(args):
    "Checks to see if the given process count is valid."
    if (isinstance(args.processes, list)):
        processes = args.processes[0]
    else:
        processes = int(args.processes)
    args.processes = processes
    return processes

def checkScale(args):
    "Checks to see if the given scale is valid for the given region.  Returns scale and multiplier."
    fullScale = 1 # don't want higher resolution than reality!
    if (isinstance(args.scale, list)):
        oldscale = args.scale[0]
    else:
        oldscale = int(args.scale)
    lcds, elevds = getDataset(args.region)
    elevds = None
    lcperpixel = lcds.transforms[2][1]
    lcds = None
    scale = min(oldscale, lcperpixel)
    scale = max(scale, fullScale)
    if (scale != oldscale):
        print "Warning: scale of %d for region %s is invalid -- changed to %d" % (oldscale, args.region, scale)
    mult = lcperpixel/scale
    args.scale = scale
    args.mult = mult
    return (scale, mult)

def checkVScale(args):
    "Checks to see if the given vScale is valid for the given region."
    if (isinstance(args.vscale, list)):
        oldvscale = args.vscale[0]
    else:
        oldvscale = int(args.vscale)
    (lcds, elevds) = getDataset(args.region)
    lcds = None
    elevBand = elevds.GetRasterBand(1)
    elevCMinMax = elevBand.ComputeRasterMinMax(False)
    elevBand = None
    elevds = None
    elevMax = elevCMinMax[1]
    vscale = min(oldvscale, elevMax)
    vscale = max(vscale, (elevMax/maxelev)+1)
    if (vscale != oldvscale):
        print "Warning: vertical scale of %d for region %s is invalid -- changed to %d" % (oldvscale, args.region, vscale)
    args.vscale = vscale
    return vscale

def main(argv):
    "The main portion of the script."

    default_scale = 6
    default_vscale = 6
    default_tile = [256, 256]
    default_start = [0, 0]
    default_end = [0, 0]
    default_processes = cpu_count()

    parser = ArgumentParser(description='Generate images for BuildWorld.js from USGS datasets.')
    parser.add_argument('--region', nargs='?', type=checkDataset, help='a region to be processed (leave blank for list of regions)')
    parser.add_argument('--processes', nargs=1, default=default_processes, type=int, help="number of processes to spawn (default %d)" % default_processes)
    parser.add_argument('--scale', nargs=1, default=default_scale, type=int, help="horizontal scale factor (default %d)" % default_scale)
    parser.add_argument('--vscale', nargs=1, default=default_vscale, type=int, help="vertical scale factor (default %d)" % default_vscale)
    parser.add_argument('--maxdepth', nargs=1, default=maxdepth, type=checkMaxdepth, help="maximum depth (default %d)" % maxdepth)
    parser.add_argument('--slope', nargs=1, default=slope, type=checkSlope, help="underwater slope factor (default %d)" % slope)
    parser.add_argument('--tile', nargs=2, default=default_tile, type=int, help="tile size in tuple form (default %s)" % (default_tile,))
    parser.add_argument('--start', nargs=2, default=default_start, type=int, help="start tile in tuple form (default %s)" % (default_start,))
    parser.add_argument('--end', nargs=2, default=default_end, type=int, help="end tile in tuple form (default %s)" % (default_end,))
    args = parser.parse_args()

    # list regions if requested
    if (args.region == None):
        listDatasets(dsDict)
        return 0

    # set up all the values
    rows, cols = getDatasetDims(args.region)
    processes = checkProcesses(args)
    (scale, mult) = checkScale(args)
    vscale = checkVScale(args)
    tileShape = checkTile(args, mult)
    (tileRows, tileCols) = tileShape
    (minTileRows, minTileCols, maxTileRows, maxTileCols) = checkStartEnd(args, mult, tileShape)

    print "Processing region %s of size (%d, %d) with %d processes..." % (args.region, rows, cols, processes)

    # createArrays should:
    # create the shared memory arrays like initWorld
    minX = 0
    minZ = 0
    maxX = int(rows*mult)
    maxZ = int(cols*mult)
    createArrays(minX, minZ, maxX, maxZ)

    # build crust tree for whole map
    makeCrustIDT(args)

    # process those tiles
    peaks = processTiles(args, minTileRows, maxTileRows, minTileCols, maxTileCols, processes)

    print "... tiles completed: total array of %d tiles was %d x %d" % ((maxTileRows-minTileRows)*(maxTileCols-minTileCols), int(rows*mult), int(cols*mult))

    # per-tile peaks here
    # ... consider doing something nice on all the peaks?
    peak = sorted(peaks, key=lambda point: point[2], reverse=True)[0]

    # where's that ore?
    placeOre()

    # place the safehouse at the peak (adjust it)
    building(peak[0], peak[1], peak[2]-1, 7, 9, 8, 1)
    print "Consider setting spawn point to %d, %d, %d" % (peak[0], peak[2]+1, peak[1])

    # save arrays
    arraydir = os.path.join("Arrays", args.region)
    saveArrays(arraydir, processes)

if __name__ == '__main__':
    sys.exit(main(sys.argv))
        
