#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.WARNING)
import sys
import argparse
import os
import yaml
from klogger import klogger, klog_levels

def main():
    """Rebuilds maps on broken regions."""
    parser = argparse.ArgumentParser(description='Prepares downloaded regions for building.')
    parser.add_argument('--name', required=True, type=str, help='name of region')
    parser.add_argument('--disable-opencl', action='store_false', dest='doOCL', default=True, help='disable OpenCL code')
    parser.add_argument('--single', action='store_true', help='enable single-threaded mode for debugging or profiling')
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

    log.log_info("Preparing region %s..." % args.name)
    yamlfile = file(os.path.join('Regions', args.name, 'Region.yaml'))
    myRegion = yaml.load(yamlfile)
    yamlfile.close()

    myRegion.log = log
    myRegion.buildmap(args.doOCL, args.single)

if __name__ == '__main__':
    sys.exit(main())
