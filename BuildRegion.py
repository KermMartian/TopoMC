#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.WARNING)
from tile import Tile
from utils import setspawnandsave, save, cleanmkdir
import argparse
import os
import shutil
import yaml
from multiprocessing import Pool
#from multiprocessing.pool import AsyncResult
from itertools import product
from tree import Tree, treeObjs
from ore import Ore, oreObjs
from pymclevel import mclevel, box
import time
from math import floor, ceil
import fnmatch

def buildtile(args):
    """Given a region name and coordinates, build the corresponding tile."""
    # this should work for single and multi threaded cases
    (name, tilex, tiley) = args
    print("Building tile (%d,%d) of map %s..." % \
          (tilex, tiley, name))
    yamlfile = file(os.path.join('Regions', name, 'Region.yaml'))
    myRegion = yaml.load(yamlfile)
    yamlfile.close()
    myTile = Tile(myRegion, tilex, tiley)
    myTile()

def buildregion(args):
    """Given a region name and coordinates, build the corresponding region from tile chunks."""
    # this should work for single and multi threaded cases
    (name, regionx, regiony) = args
    print("Building region (%d,%d) of map %s..." % \
          (regionx, regiony, name))
    yamlfile = file(os.path.join('Regions', name, 'Region.yaml'))
    myRegion = yaml.load(yamlfile)
    yamlfile.close()
    regionsize = 32 * 16
    tilexrange = xrange(regionx * (regionsize / myRegion.tilesize), \
                        (regionx + 1) * (regionsize / myRegion.tilesize))
    tileyrange = xrange(regiony * (regionsize / myRegion.tilesize), \
                        (regiony + 1) * (regionsize / myRegion.tilesize))
    maintiledir = None
    for tilex, tiley in product(tilexrange, tileyrange):
        if (tilex < myRegion.tiles['xmin']) or (tilex >= myRegion.tiles['xmax']) or \
           (tiley < myRegion.tiles['ymin']) or (tiley >= myRegion.tiles['ymax']):
            continue
        tiledir = os.path.join('Regions', name, 'Tiles', '%dx%d' % (tilex, tiley))
        if not(os.path.exists(tiledir)) or not(os.path.exists(os.path.join(tiledir, 'level.dat'))):
            print("Skipping missing tile" % (tilex, tiley))
            continue
        if maintiledir == None:
            maintiledir = tiledir
            maintileworld = mclevel.MCInfdevOldLevel(tiledir, create=False)
            # If this hack is necessary, it's dumb. It seems to be.
            maintileworld.copyBlocksFrom(maintileworld, maintileworld.bounds, \
                                         maintileworld.bounds.origin)
        else:
            print("Copying tile %dx%d to %s" % (tilex, tiley, maintiledir))
            tileworld = mclevel.MCInfdevOldLevel(tiledir, create=False)
            maintileworld.copyBlocksFrom(tileworld, tileworld.bounds, tileworld.bounds.origin)
            tileworld = None

    if maintiledir == None:
        print("Region (%d,%d) appears to be complete; skipping." % (regionx, regiony))
        return
    else:
        print("maintiledir is %s" % maintiledir)

    save(maintileworld)
    maintileworld = None
    srcdir = os.path.join(maintiledir, 'region')
    dstdir = os.path.join('Worlds', name, 'region')
    for path, dirlist, filelist in os.walk(srcdir):
        for name in fnmatch.filter(filelist, "*.mca"):
            shutil.move(os.path.join(path,name),dstdir)
            print("Moved file %s to %s" % (os.path.join(path,name),dstdir))
    os.remove(os.path.join(maintiledir,'level.dat'))

def main():
    """Builds a region."""
    # example:
    # ./BuildRegion.py --name BlockIsland

    # parse options and get results
    parser = argparse.ArgumentParser(description='Builds Minecraft worlds from regions.')
    parser.add_argument('--name', required=True, type=str, help='name of the region to be built')
    parser.add_argument('--debug', action='store_true', help='enable debug output')
    parser.add_argument('--single', action='store_true', help='enable single-threaded mode for debugging or profiling')
    args = parser.parse_args()

    # enable debug
    if (args.debug):
        print "Do something!"

    # build the region
    print "Building region %s..." % args.name
    yamlfile = file(os.path.join('Regions', args.name, 'Region.yaml'))
    myRegion = yaml.load(yamlfile)
    yamlfile.close()

    # exit if map does not exist
    if not os.path.exists(myRegion.mapname):
        raise IOError, "no map file exists"

    # tree and ore variables
    treeobjs = dict([(tree.name, tree) for tree in treeObjs])
    trees = dict([(name, list()) for name in treeobjs])
    oreobjs = dict([(ore.name, ore) for ore in oreObjs])
    ores = dict([(name, list()) for name in oreobjs])

    # generate overall world
    worlddir = os.path.join('Worlds', args.name)
    world = mclevel.MCInfdevOldLevel(worlddir, create=True)
    peak = [0, 0, 0]
    save(world)
    world = None

    # generate individual tiles
    tilexrange = xrange(myRegion.tiles['xmin'], myRegion.tiles['xmax'])
    tileyrange = xrange(myRegion.tiles['ymin'], myRegion.tiles['ymax'])
    name = myRegion.name
    tiles = [(name, x, y) for x, y in product(tilexrange, tileyrange)]
    if args.single:
        # single process version - works
        for tile in tiles:
            buildtile(tile)
    else:
        # multi-process ... let's see...
        pool = Pool()
        rs = pool.map_async(buildtile, tiles)
        pool.close()
        #pool.join()
        while not(rs.ready()):
            remaining = rs._number_left
            print "Waiting for " + str(remaining) + " buildtile tasks to complete..."
            time.sleep(10)
        pool.join()        # Just as a precaution.

    # Generate regions
    regionsize = 32 * 16
    regionxrange = xrange(int(floor(myRegion.tiles['xmin'] * (myRegion.tilesize / float(regionsize)))), \
                          int(ceil(myRegion.tiles['xmax'] * (myRegion.tilesize / float(regionsize)))))
    regionyrange = xrange(int(floor(myRegion.tiles['ymin'] * (myRegion.tilesize / float(regionsize)))), \
                          int(ceil(myRegion.tiles['ymax'] * (myRegion.tilesize / float(regionsize)))))

    regions = [(name, x, y) for x, y in product(regionxrange, regionyrange)]

    # merge individual tiles into regions
    print "Merging %d tiles into one world..." % len(tiles)
    for tile in tiles:
        (name, x, y) = tile
        tiledir = os.path.join('Regions', name, 'Tiles', '%dx%d' % (x, y))
        if not(os.path.isfile(os.path.join(tiledir, 'Tile.yaml'))):
            print("The following tile is missing. Please re-run this script:\n%s" % \
                  os.path.join(tiledir, 'Tile.yaml'))
            raise IOError

    # Necessary for tile-welding -> regions
    cleanmkdir(worlddir)
    cleanmkdir(os.path.join(worlddir, 'region'))

    if args.single:
        # single process version - works
        for region in regions:
            buildregion(region)
    else:
        # multi-process ... let's see...
        pool = Pool()
        rs = pool.map_async(buildregion, regions)
        pool.close()
        #pool.join()
        while not(rs.ready()):
            remaining = rs._number_left
            print "Waiting for " + str(remaining) + " buildregion tasks to complete..."
            time.sleep(10)
        pool.join()        # Just as a precaution.

    world = mclevel.MCInfdevOldLevel(worlddir, create=True)
    mcoffsetx = myRegion.tiles['xmin'] * myRegion.tilesize
    mcoffsetz = myRegion.tiles['ymin'] * myRegion.tilesize
    mcsizex   = (myRegion.tiles['xmax'] - myRegion.tiles['xmin']) * myRegion.tilesize
    mcsizez   = (myRegion.tiles['ymax'] - myRegion.tiles['ymin']) * myRegion.tilesize
    tilebox = box.BoundingBox((mcoffsetx, 0, mcoffsetz), (mcsizex, world.Height, mcsizez))
    world.createChunksInBox(tilebox)

    for tile in tiles:
        (name, x, y) = tile
        tiledir = os.path.join('Regions', name, 'Tiles', '%dx%d' % (x, y))
        tilefile = file(os.path.join(tiledir, 'Tile.yaml'))
        newtile = yaml.load(tilefile)
        tilefile.close()
        if (newtile.peak[1] > peak[1]):
            peak = newtile.peak
        for treetype in newtile.trees:
            trees.setdefault(treetype, []).extend(newtile.trees[treetype])
        if myRegion.doOre:
            for oretype in newtile.ores:
                ores.setdefault(oretype, []).extend(newtile.ores[oretype])
        #tileworld = mclevel.MCInfdevOldLevel(tiledir, create=False)
        #world.copyBlocksFrom(tileworld, tileworld.bounds, tileworld.bounds.origin)
        #tileworld = False

    # plant trees in our world
    print "Planting %d trees at the region level..." % sum([len(trees[treetype]) for treetype in trees])
    Tree.placetreesinregion(trees, treeobjs, world)

    # deposit ores in our world
    if myRegion.doOre:
        print "Depositing %d ores at the region level..." % sum([len(ores[oretype]) for oretype in ores])
        Ore.placeoreinregion(ores, oreobjs, world)

    # tie up loose ends
    world.setPlayerGameType(1)
    setspawnandsave(world, peak)
    oldyamlpath = os.path.join('Regions', args.name, 'Region.yaml')
    newyamlpath = os.path.join('Worlds', args.name, 'Region.yaml')
    shutil.copy(oldyamlpath, newyamlpath)
    shutil.rmtree(os.path.join('Regions', name, 'Tiles'))

if __name__ == '__main__':
    main()

