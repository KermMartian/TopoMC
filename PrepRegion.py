#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.WARNING)
import sys
import argparse
import os
import yaml

def main():
    """Rebuilds maps on broken regions."""
    parser = argparse.ArgumentParser(description='Prepares downloaded regions for building.')
    parser.add_argument('--name', required=True, type=str, help='name of region')
    parser.add_argument('--disable-opencl', action='store_false', dest='doOCL', default=True, help='disable OpenCL code')
    parser.add_argument('--single', action='store_true', help='enable single-threaded mode for debugging or profiling')

    args = parser.parse_args()

    print "Preparing region %s..." % args.name
    yamlfile = file(os.path.join('Regions', args.name, 'Region.yaml'))
    myRegion = yaml.load(yamlfile)
    yamlfile.close()

    myRegion.buildmap(args.doOCL, args.single)

if __name__ == '__main__':
    sys.exit(main())
