#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.WARNING)
from region import Region
import sys
import argparse
from klogger import klogger, klog_levels

def checkOrthoIDs(string):
    """Checks to see if the given product IDs are valid."""
    givenIDs = string.split(',')
    validIDs = [ ID for ID in givenIDs if ID in Region.productIDs['ortho'] ]
    if validIDs == givenIDs:
        return givenIDs
    else:
        raise argparse.ArgumentTypeError, 'ortho IDs invalid: %s' % string

def checkElevationIDs(string):
    """Checks to see if the given product IDs are valid."""
    givenIDs = string.split(',')
    validIDs = [ ID for ID in givenIDs if ID in Region.productIDs['elevation'] ]
    if validIDs == givenIDs:
        return givenIDs
    else:
        raise argparse.ArgumentTypeError, 'elevation IDs invalid: %s' % string

def checkLandcoverIDs(string):
    """Checks to see if the given product IDs are valid."""
    givenIDs = string.split(',')
    validIDs = [ ID for ID in givenIDs if ID in Region.productIDs['landcover'] ]
    if validIDs == givenIDs:
        return givenIDs
    else:
        raise argparse.ArgumentTypeError, 'landcover IDs invalid: %s' % string

def main():
    """Creates a specified region and downloads files from USGS."""
    # example:
    # ./GetRegion.py --name BlockIsland --ymax 41.2378 --ymin 41.1415 --xmin -71.6202 --xmax -71.5332

    # defaults
    default_orthoIDs     = ','.join(Region.productIDs['ortho'])
    default_elevationIDs = ','.join(Region.productIDs['elevation'])
    default_landcoverIDs = ','.join(Region.productIDs['landcover'])

    # parse options and get results
    parser = argparse.ArgumentParser(description='Create regions and download files from USGS.')
    parser.add_argument('--name', required=True, type=str, help='name of the region to be generated')
    parser.add_argument('--xmax', required=True, type=float, help='easternmost longitude (west is negative)')
    parser.add_argument('--xmin', required=True, type=float, help='westernmost longitude (west is negative)')
    parser.add_argument('--ymax', required=True, type=float, help='northernmost latitude (south is negative)')
    parser.add_argument('--ymin', required=True, type=float, help='southernmost longitude (south is negative)')
    parser.add_argument('--tilesize', type=int, help='tilesize value (default %d)' % Region.tilesize)
    parser.add_argument('--scale', type=int, help='scale value (default %d)' % Region.scale)
    parser.add_argument('--vscale', type=int, help='vscale value (default %d)' % Region.vscale)
    parser.add_argument('--trim', type=int, help='trim value (default %d)' % Region.trim)
    parser.add_argument('--sealevel', type=int, help='sealevel value (default %d)' % Region.sealevel)
    parser.add_argument('--maxdepth', type=int, help='maxdepth value (default %d)' % Region.maxdepth)
    parser.add_argument('--orthoIDs', default=default_orthoIDs, type=checkOrthoIDs, help='ordered list of product IDs (default %s)' % default_orthoIDs)
    parser.add_argument('--elevationIDs', default=default_elevationIDs, type=checkElevationIDs, help='ordered list of product IDs (default %s)' % default_elevationIDs)
    parser.add_argument('--landcoverIDs', default=default_landcoverIDs, type=checkLandcoverIDs, help='ordered list of product IDs (default %s)' % default_landcoverIDs)
    parser.add_argument('--enable-ore', action='store_true', dest='doOre', default=False, help='enable ore generation')
    parser.add_argument('--enable-schematics', action='store_true', dest='doSchematics', default=False, help='enable schematic usage')
    parser.add_argument("-v", "--verbosity", action="count", \
                        help="increase output verbosity")
    parser.add_argument("-q", "--quiet", action="store_true", \
                        help="suppress informational output")
    args = parser.parse_args()

    # set up logging
    log_level = klog_levels.LOG_INFO
    if args.quiet:
        log_level = klog_levels.LOG_ERROR
    if args.verbosity:
        # v=1 is DEBUG 1, v=2 is DEBUG 2, and so on
        log_level += args.verbosity
    log = klogger(log_level)

    # create the region
    log.log_info("Creating new region %s..." % args.name)
    myRegion = Region(name=args.name, xmax=args.xmax, xmin=args.xmin, ymax=args.ymax, ymin=args.ymin, scale=args.scale, vscale=args.vscale, trim=args.trim, tilesize=args.tilesize, sealevel=args.sealevel, maxdepth=args.maxdepth, oiIDs=args.orthoIDs, lcIDs=args.landcoverIDs, elIDs=args.elevationIDs, doOre=args.doOre, doSchematics=args.doSchematics)

    log.log_info("Retrieving files...")
    myRegion.log = log
    myRegion.getfiles()

if __name__ == '__main__':
    sys.exit(main())

