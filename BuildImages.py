#!/usr/bin/env python
# mklcelevdata.py - 2010Jan21 - mathuin@gmail.com

# this script builds arrays for land cover and elevation

from __future__ import division
import os
import fnmatch
import sys
import struct
import numpy
import Image
import argparse
from osgeo import gdal
from osgeo import osr
from osgeo.gdalconst import *
from invdisttree import *
gdal.UseExceptions()

# functions
def locateDataset(region, prefix=""):
    "Given a region name and an optional prefix, returns the dataset for that region."
    # NB: assumed that exactly one dataset exists for each region/prefix
    dsfilename = ''
    dspaths = ['Datasets', '../TopoMC-Datasets']
    for dspath in dspaths:
        for path, dirs, files in os.walk(os.path.abspath((dspath+'/'+region))):
            for filename in fnmatch.filter(files, prefix+"[0-9]*.tif"):
                dsfilename = os.path.join(path, filename)
    # no dataset found
    if (dsfilename == ''):
        return None
    return gdal.Open(dsfilename, GA_ReadOnly)

def getIDT(ds, offset, size, vScale=1):
    "Convert a portion of a given dataset (identified by corners) to an inverse distance tree."
    # retrieve data from dataset
    (Trans, ArcTrans, GeoTrans) = getTransforms(ds)
    Band = ds.GetRasterBand(1)
    Data = Band.ReadAsArray(offset[0], offset[1], size[0], size[1])
    Band = None

    # build initial arrays
    LatLong = getLatLongArray(Trans, GeoTrans, (offset), (size), 1)
    Value = Data.flatten()

    # scale elevation vertically
    Value = Value / vScale

    # build tree
    IDT = Invdisttree(LatLong, Value)

    return IDT

def getLatLongArray(transform, geotransform, offset, size, mult=1):
    "Given transformations, dimensions, and multiplier, generate the interpolated array."
    rows = list(numpy.linspace(offset[1]/mult, (offset[1]+size[1])/mult, size[1], False))
    cols = list(numpy.linspace(offset[0]/mult, (offset[0]+size[0])/mult, size[0], False))
    retval = numpy.array([getLatLong(transform, geotransform, row, col) for row in rows for col in cols])

    return retval

def getTransforms(ds):
    "Given a dataset, return the transform and geotransform."
    Projection = ds.GetProjectionRef()
    Proj = osr.SpatialReference(Projection)
    LatLong = Proj.CloneGeogCS()
    Trans = osr.CoordinateTransformation(Proj, LatLong)
    ArcTrans = osr.CoordinateTransformation(LatLong, Proj)
    GeoTrans = ds.GetGeoTransform()

    return Trans, ArcTrans, GeoTrans

def getLatLong(transform, geotransform, x, y):
    "Given transform, geotransform, and coordinates, return latitude and longitude.  Based on GDALInfoReportCorner() from gdalinfo.py"
    dfGeoX = geotransform[0] + geotransform[1] * x + geotransform[2] * y
    dfGeoY = geotransform[3] + geotransform[4] * x + geotransform[5] * y
    pnt = transform.TransformPoint(dfGeoX, dfGeoY, 0)
    return pnt[1], pnt[0]

def getOffsetSize(ds, corners, mult=1):
    "Convert corners to offset and size."
    (ul, lr) = corners
    (Trans, ArcTrans, GeoTrans) = getTransforms(ds)
    offset_x, offset_y = getCoords(ArcTrans, GeoTrans, ul[0], ul[1])
    if (offset_x < 0):
        offset_x = 0
    if (offset_y < 0):
        offset_y = 0
    farcorner_x, farcorner_y = getCoords(ArcTrans, GeoTrans, lr[0], lr[1])
    if (farcorner_x > ds.RasterXSize):
        farcorner_x = ds.RasterXSize
    if (farcorner_y > ds.RasterYSize):
        farcorner_y = ds.RasterYSize
    offset = (int(offset_x*mult), int(offset_y*mult))
    size = (farcorner_x*mult-offset_x*mult, farcorner_y*mult-offset_y*mult)
    if (size[0] < 0 or size[1] < 0):
        print "DEBUG: negative size is bad!"
    return offset, size

def getCoords(transform, geotransform, lat, lon):
    "The backwards version of getLatLong, from geo_trans.c."
    pnt = transform.TransformPoint(lon, lat, 0)
    x = (pnt[0] - geotransform[0])/geotransform[1]
    y = (pnt[1] - geotransform[3])/geotransform[5]
    return int(x), int(y)

def getImageArray(ds, idtCorners, baseArray, nnear, vScale=1):
    "Given the relevant information, builds the image array."

    Offset, Size = getOffsetSize(ds, idtCorners)
    IDT = getIDT(ds, Offset, Size, vScale)
    ImageArray = IDT(baseArray, nnear=nnear, eps=.1)

    return ImageArray

def getTileOffsetSize(rowIndex, colIndex, tileShape, maxRows, maxCols, mult=1, idtPad=0):
    "run this with idtPad=0 to generate image."
    imageRows = tileShape[0]
    imageCols = tileShape[1]
    imageLeft = rowIndex*imageRows-idtPad
    imageRight = imageLeft+imageRows+2*idtPad
    imageUpper = colIndex*imageCols-idtPad
    imageLower = imageUpper+imageCols+2*idtPad
    if (imageLeft < 0):
        imageLeft = 0
    if (imageRight > maxRows):
        imageRight = maxRows
    if (imageUpper < 0):
        imageUpper = 0
    if (imageLower > maxCols):
        imageLower = maxCols
    imageOffset = (imageLeft, imageUpper)
    imageSize = (imageRight-imageLeft, imageLower-imageUpper)
    return imageOffset, imageSize

def xytuple(string):
    #split string on comma
    errorMsg = "not a suitable tuple: %s" % string
    (first, sep, last) = string.partition(",")
    if (not(sep == "," and first.isdigit() and last.isdigit())):
        raise argparse.ArgumentTypeError(errorMsg)
    return (int(first), int(last))

# main
def main(argv):
    "The main portion of the script."

    parser = argparse.ArgumentParser(description='Generate images for BuildWorld.js from USGS datasets.')
    # TODO: find them and list them
    # for now, having just one and it being the default is keen
    # http://docs.python.org/library/argparse.html#type
    # do perfect_square with locateDataset?!
    parser.add_argument('-region', nargs='?', default='BlockIsland', help='a region to be processed')
    parser.add_argument('-scale', nargs='?', default=6, help="horizontal scale factor")
    parser.add_argument('-vscale', nargs='?', default=6, help="vertical scale factor")
    parser.add_argument('-tilesize', nargs='?', default='256,256', type=xytuple, help="tile size in tuple form")
    parser.add_argument('-start', nargs='?', default='0,0', type=xytuple, help="starting tile in tuple form")
    parser.add_argument('-end', nargs='?', default='0,0', type=xytuple, help="ending tile in tuple form")
    args = parser.parse_args()

    # lazy or ugly?
    region = args.region
    scale = args.scale
    vscale = args.vscale
    tileShape = args.tilesize
    (tileRows, tileCols) = tileShape
    (minTileRows, minTileCols) = args.start
    (maxTileRows, maxTileCols) = args.end

    # locate datasets
    lcds = locateDataset(region)
    if (lcds == None):
        print "Error: no land cover dataset found matching %s!" % region
        sys.exit()
    elevds = locateDataset(region, 'NED_')
    if (elevds == None):
        print "Error: no elevation dataset found matching %s!" % region
        sys.exit()

    # make imagedir
    # TODO: add error checking
    imagedir = os.path.join("Images", region)
    if not os.path.exists(imagedir):
        os.makedirs(imagedir)

    print "Processing region %s..." % region
    # do both datasets have the same projection?
    lcGeogCS = osr.SpatialReference(lcds.GetProjectionRef()).CloneGeogCS()
    elevGeogCS = osr.SpatialReference(elevds.GetProjectionRef()).CloneGeogCS()

    if (not lcGeogCS.IsSameGeogCS(elevGeogCS)):
        print "Error: land cover and elevation maps do not have the same projection."
        sys.exit()

    # set up scaling factors
    # horizontal based on land cover
    lcTrans, lcArcTrans, lcGeoTrans = getTransforms(lcds)
    lcperpixel = lcGeoTrans[1]
    mult = lcperpixel//scale
    # vertical based on elevation
    elevBand = elevds.GetRasterBand(1)
    elevCMinMax = elevBand.ComputeRasterMinMax(False)
    elevBand = None
    elevMax = elevCMinMax[1]
    if (elevMax/vscale > 60):
        vscale = int(elevMax/60)-1

    # NEW IDEA
    maxRows = lcds.RasterXSize*mult
    maxCols = lcds.RasterYSize*mult
    numRowTiles = int((maxRows+tileRows-1)/tileRows)
    numColTiles = int((maxCols+tileCols-1)/tileCols)
    if (maxTileRows == 0 or maxTileRows > numRowTiles):
        maxTileRows = numRowTiles
    if (minTileRows > maxTileRows):
        minTileRows = maxTileRows
    if (maxTileCols == 0 or maxTileCols > numColTiles):
        maxTileCols = numColTiles
    if (minTileCols > maxTileCols):
        minTileCols = maxTileCols
    for tileRowIndex in range(minTileRows, maxTileRows):
        for tileColIndex in range(minTileRows, maxTileRows):
            baseOffset, baseSize = getTileOffsetSize(tileRowIndex, tileColIndex, tileShape, maxRows, maxCols)
            idtOffset, idtSize = getTileOffsetSize(tileRowIndex, tileColIndex, tileShape, maxRows, maxCols, idtPad=16)
            print "Generating tile (%d, %d) with dimensions (%d, %d)..." % (tileRowIndex, tileColIndex, baseSize[0], baseSize[1])

            baseShape = (baseSize[1], baseSize[0])
            baseArray = getLatLongArray(lcTrans, lcGeoTrans, baseOffset, baseSize, mult)

            # these points are scaled coordinates
            idtUL = getLatLong(lcTrans, lcGeoTrans, idtOffset[0]/mult, idtOffset[1]/mult)
            idtLR = getLatLong(lcTrans, lcGeoTrans, (idtOffset[0]+idtSize[0])/mult, (idtOffset[1]+idtSize[1])/mult)

            # nnear=1 for landcover, 11 for elevation
            lcImageArray = getImageArray(lcds, (idtUL, idtLR), baseArray, 1)
            lcImageArray.resize(baseShape)
            lcImage = Image.fromarray(lcImageArray)
            lcImage.save('%s/lc-%d-%d.gif' % (imagedir, baseOffset[0], baseOffset[1]))

            # nnear=1 for landcover, 11 for elevation
            elevImageArray = getImageArray(elevds, (idtUL, idtLR), baseArray, 11, vscale)
            elevImageArray.resize(baseShape)
            elevImage = Image.fromarray(elevImageArray)
            elevImage.save('%s/elev-%d-%d.gif' % (imagedir, baseOffset[0], baseOffset[1]))
    print "Render complete -- total array of %d tiles was %d x %d" % (numRowTiles*numColTiles, maxRows, maxCols)

if __name__ == '__main__':
    sys.exit(main(sys.argv))
