#!/usr/bin/env python

from __future__ import division
from math import ceil, floor
import math
import suds
import re
import os
import urllib2
import urlparse
from progressbar import ProgressBar, Percentage, Bar, ETA, FileTransferSpeed
import yaml
import logging
from time import sleep
import tarfile
logging.basicConfig(level=logging.INFO)
from utils import cleanmkdir
from itertools import product
from terrain import Terrain
from pymclevel import mclevel

from osgeo import gdal, osr
from osgeo.gdalconst import GDT_Int16, GA_ReadOnly
from bathy import getBathy
from crust import Crust
import numpy
#
from clidt import CLIDT

class SmartRedirectHandler(urllib2.HTTPRedirectHandler):
    """Handle temporary redirections by saving status."""
    def __init__(self):
        pass

    def http_error_302(self, req, fp, code, msg, headers):
        result = urllib2.HTTPRedirectHandler.http_error_302(self, req, fp, code, msg, headers)
        result.status = code
        result.headers = headers
        return result

class Region:
    """Primary class for regions."""

    # coordinate systems
    wgs84 = 4326
    albers = 102039
    t_srs = "+proj=aea +datum=NAD83 +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +units=m"

    # raster layer order
    rasters = {'landcover': 1, 'elevation': 2, 'bathy': 3, 'crust': 4, 'orthor': 5, 'orthog': 6, 'orthob': 7, 'orthoir': 8}
    
    # default values
    tilesize = 256
    scale = 6
    vscale = 6
    trim = 0
    sealevel = 64
    maxdepth = 32
    prepTileSize = 1000
    prepTileSizeOverlap = 1100

    # tileheight is height of map in Minecraft units TODO XXX CHANGE
    tileheight = mclevel.MCInfdevOldLevel.Height

    # headroom is room between top of terrain and top of map
    headroom = 16

    # product types in order of preference
    productIDs = { 'ortho': ['P10', 'P11', 'P12', 'P13', 'P14', 'P15', 'P16', 'P17', 'P18', 'P19', 'ONF', 'OF9'],
                   'elevation': ['N3F', 'N2F', 'N1F'],
                   'landcover': sorted(Terrain.translate.keys()) }
    seamlessIDs = ['P10', 'P11', 'P12', 'P13', 'P14', 'P15', 'P16', 'P17', 'P18']
    # file suffix for extraction
    # FIXME: check N2F value
    exsuf = { 'N3F': '13', 'N2F': '12', 'N1F': '1' }

    # download directory
    downloadtop = os.path.abspath('Downloads')
    regiontop = os.path.abspath('Regions')

    def __init__(self, name, xmax, xmin, ymax, ymin, tilesize=None, scale=None, vscale=None, trim=None, sealevel=None, maxdepth=None, oiIDs=None, lcIDs=None, elIDs=None, doOre=True, doSchematics=False):
        """Create a region based on lat-longs and other parameters."""
        # NB: smart people check names
        self.name = name

        # tile must be an even multiple of chunk width
        # chunkWidth not defined in pymclevel but is hardcoded everywhere
        if tilesize == None:
            tilesize = Region.tilesize
        else:
            if (tilesize % 16 != 0):
                raise AttributeError, 'bad tilesize %s' % tilesize
        self.tilesize = tilesize

        # scale can be any positive integer
        if scale == None:
            scale = Region.scale
        else:
            if (scale > 0):
                self.scale = int(scale)
            else:
                raise AttributeError, 'bad scale %s' % scale

        # sealevel and maxdepth are not checked until after files are retrieved
        if sealevel == None:
            sealevel = Region.sealevel
        else:
            self.sealevel = sealevel

        if maxdepth == None:
            maxdepth = Region.maxdepth
        else:
            self.maxdepth = maxdepth

        # trim and vscale are not checked until after files are retrieved
        if trim == None:
            trim = Region.trim
        else:
            self.trim = trim

        if vscale == None:
            vscale = Region.vscale
        else:
            self.vscale = vscale

        # disable overly dense elevation products
        self.productIDs = Region.productIDs
        # NB: ND9 no longer supported
        # if (scale > 5):
        #     self.productIDs['elevation'].remove('ND9')
        if (scale > 15):
            self.productIDs['elevation'].remove('N3F')

        # specified IDs must be in region list
        if oiIDs == None:
            orthoIDs = self.productIDs['ortho']
        else:
            orthoIDs = [ ID for ID in oiIDs if ID in self.productIDs['ortho'] ]
            if orthoIDs == []:
                raise AttributeError, 'invalid ortho ID'

        if lcIDs == None:
            landcoverIDs = self.productIDs['landcover']
        else:
            landcoverIDs = [ ID for ID in lcIDs if ID in self.productIDs['landcover'] ]
            if landcoverIDs == []:
                raise AttributeError, 'invalid landcover ID'

        if elIDs == None:
            elevationIDs = self.productIDs['elevation']
        else:
            elevationIDs = [ ID for ID in elIDs if ID in self.productIDs['elevation'] ]
            if elevationIDs == []:
                raise AttributeError, 'invalid elevation ID'

        # enable or disable ore and schematics
        self.doOre = doOre
        self.doSchematics = doSchematics

        # crazy directory fun
        self.regiondir = os.path.join(Region.regiontop, self.name)
        cleanmkdir(self.regiondir)

        self.mapsdir = os.path.join(self.regiondir, 'Datasets')
        cleanmkdir(self.mapsdir)

        self.mapname = os.path.join(self.regiondir, 'Map.tif')

        # these are the latlong values
        self.llextents = { 'xmax': max(xmax, xmin), 'xmin': min(xmax, xmin), 'ymax': max(ymax, ymin), 'ymin': min(ymax, ymin) }

        # access the web service
        # NB: raise hell if it is inaccessible
        wsdlConv = "http://extract.cr.usgs.gov/XMLWebServices/Coordinate_Conversion_Service.asmx?WSDL"
        clientConv = suds.client.Client(wsdlConv)
        # This web service returns suds.sax.text.Text not XML sigh
        Convre = "<X Coordinate>(.*?)</X Coordinate > <Y Coordinate>(.*?)</Y Coordinate >"

        # convert from WGS84 to Albers
        ULdict = {'X_Value': self.llextents['xmin'], 'Y_Value': self.llextents['ymin'], 'Current_Coordinate_System': Region.wgs84, 'Target_Coordinate_System': Region.albers}
        (ULx, ULy) = re.findall(Convre, clientConv.service.getCoordinates(**ULdict))[0]
        URdict = {'X_Value': self.llextents['xmax'], 'Y_Value': self.llextents['ymin'], 'Current_Coordinate_System': Region.wgs84, 'Target_Coordinate_System': Region.albers}
        (URx, URy) = re.findall(Convre, clientConv.service.getCoordinates(**URdict))[0]
        LLdict = {'X_Value': self.llextents['xmin'], 'Y_Value': self.llextents['ymax'], 'Current_Coordinate_System': Region.wgs84, 'Target_Coordinate_System': Region.albers}
        (LLx, LLy) = re.findall(Convre, clientConv.service.getCoordinates(**LLdict))[0]
        LRdict = {'X_Value': self.llextents['xmax'], 'Y_Value': self.llextents['ymax'], 'Current_Coordinate_System': Region.wgs84, 'Target_Coordinate_System': Region.albers}
        (LRx, LRy) = re.findall(Convre, clientConv.service.getCoordinates(**LRdict))[0]

        # select maximum values for landcover extents
        xfloat = [float(x) for x in [ULx, URx, LLx, LRx]]
        yfloat = [float(y) for y in [ULy, URy, LLy, LRy]]
        mxmax = max(xfloat)
        mxmin = min(xfloat)
        mymax = max(yfloat)
        mymin = min(yfloat)

        # calculate tile edges
        realsize = self.scale * self.tilesize
        self.tiles = { 'xmax': int(ceil(mxmax / realsize)), 'xmin': int(floor(mxmin / realsize)), 'ymax': int(ceil(mymax / realsize)), 'ymin': int(floor(mymin / realsize)) }

        self.albersextents = { 'ortho': dict(), 'landcover': dict(), 'elevation': dict() }
        self.wgs84extents = { 'ortho': dict(), 'landcover': dict(), 'elevation': dict() }

        # landcover has a maxdepth-sized border
        self.albersextents['elevation'] = { 'xmax': self.tiles['xmax'] * realsize, 'xmin': self.tiles['xmin'] * realsize, 'ymax': self.tiles['ymax'] * realsize, 'ymin': self.tiles['ymin'] * realsize }
        borderwidth = self.maxdepth * self.scale
        self.albersextents['landcover'] = { 'xmax': self.albersextents['elevation']['xmax'] + borderwidth, 'xmin': self.albersextents['elevation']['xmin'] - borderwidth, 'ymax': self.albersextents['elevation']['ymax'] + borderwidth, 'ymin': self.albersextents['elevation']['ymin'] - borderwidth }
        self.albersextents['ortho'] = { 'xmax': self.tiles['xmax'] * realsize, 'xmin': self.tiles['xmin'] * realsize, 'ymax': self.tiles['ymax'] * realsize, 'ymin': self.tiles['ymin'] * realsize }

        # now convert back from Albers to WGS84
        for maptype in ['ortho', 'landcover', 'elevation']:
            ULdict = {'X_Value': self.albersextents[maptype]['xmin'], \
                      'Y_Value': self.albersextents[maptype]['ymin'], \
                      'Current_Coordinate_System': Region.albers, \
                      'Target_Coordinate_System': Region.wgs84}
            (ULx, ULy) = re.findall(Convre, clientConv.service.getCoordinates(**ULdict))[0]
            URdict = {'X_Value': self.albersextents[maptype]['xmax'], \
                      'Y_Value': self.albersextents[maptype]['ymin'], \
                      'Current_Coordinate_System': Region.albers, \
                      'Target_Coordinate_System': Region.wgs84}
            (URx, URy) = re.findall(Convre, clientConv.service.getCoordinates(**URdict))[0]
            LLdict = {'X_Value': self.albersextents[maptype]['xmin'], \
                      'Y_Value': self.albersextents[maptype]['ymax'], \
                      'Current_Coordinate_System': Region.albers, \
                      'Target_Coordinate_System': Region.wgs84}
            (LLx, LLy) = re.findall(Convre, clientConv.service.getCoordinates(**LLdict))[0]
            LRdict = {'X_Value': self.albersextents[maptype]['xmax'], \
                      'Y_Value': self.albersextents[maptype]['ymax'], \
                      'Current_Coordinate_System': Region.albers, \
                      'Target_Coordinate_System': Region.wgs84}
            (LRx, LRy) = re.findall(Convre, clientConv.service.getCoordinates(**LRdict))[0]

            # select maximum values
            xfloat = [float(x) for x in [ULx, URx, LLx, LRx]]
            yfloat = [float(y) for y in [ULy, URy, LLy, LRy]]
            self.wgs84extents[maptype] = { 'xmax': max(xfloat), 'xmin': min(xfloat), 'ymax': max(yfloat), 'ymin': min(yfloat) }

        # check availability of product IDs and identify specific layer IDs
        self.oilayer = self.checkavail(orthoIDs, 'ortho')
        self.lclayer = self.checkavail(landcoverIDs, 'landcover')
        self.ellayer = self.checkavail(elevationIDs, 'elevation')

        # write the values to the file
        stream = file(os.path.join(self.regiondir, 'Region.yaml'), 'w')
        yaml.dump(self, stream)
        stream.close()

    def layertype(self, layerID):
        """Return 'elevation' or 'landcover' or 'ortho' depending on layerID."""
        return [key for key in self.productIDs.keys() if layerID in self.productIDs[key]][0]
        
    def checkavail(self, productlist, maptype):
        """Check availability with web service."""
        mapextents = self.wgs84extents[maptype]

        # access the web service to check availability
        wsdlInv = "http://ags.cr.usgs.gov/index_service/Index_Service_SOAP.asmx?WSDL"
        clientInv = suds.client.Client(wsdlInv)

        # ensure desired attributes are present
        desiredAttributes = ['PRODUCTKEY','STATUS']
        attributes = []
        attributeList = clientInv.service.return_Attribute_List()
        for attribute in desiredAttributes:
            if attribute in attributeList[0]:
                attributes.append(attribute)
        if len(attributes) != len(desiredAttributes):
            raise AttributeError, "Not all attributes found"
    
        # return_attributes arguments dictionary
        rAdict = {'Attribs': ','.join(attributes), 'XMin': mapextents['xmin'], 'XMax': mapextents['xmax'], 'YMin': mapextents['ymin'], 'YMax': mapextents['ymax'], 'EPSG': Region.wgs84}
        rAatts = clientInv.service.return_Attributes(**rAdict)
        # store offered products in a list
        offered = []
        options = []
        # this returns an array of custom attributes
        # which is apparently a ball full of crazy
        # NB: clean up this [0][0] crap!
        for elem in rAatts.ArrayOfCustomAttributes:
            if elem[0][0][0] == 'PRODUCTKEY' and \
               elem[0][1][0] == 'STATUS' and elem[0][1][1] in ['Tiled', 'Seamless']:
                if elem[0][0][1] in productlist:
                    offered.append(elem[0][0][1])
                options.append(elem[0][0][1])
        # this should extract the first
        try:
            productID = [ ID for ID in productlist if ID in offered ][0]
        except IndexError:
            raise AttributeError, "No products among (%s) are available for this location (options were %s)!" % \
                                  (", ".join(productlist), ", ".join(options))
        return productID

    def pid2FullPid(self, productID):
        # access the web service to check availability
        wsdlInv = "http://ags.cr.usgs.gov/index_service/Index_Service_SOAP.asmx?WSDL"
        clientInv = suds.client.Client(wsdlInv)

        productdict = {'ProductIDs': productID}
        doproducts = clientInv.service.return_Download_Options(**productdict) 
        [doPID, doType, doOF, doCF, doMF] = [value for (key, value) in doproducts['DownloadOptions'][0]]

        # assemble layerID
        layerID = doPID
        OFgood = [u'GeoTIFF']
        MFgood = [u'HTML', u'ALL', u'FAQ', u'SGML', u'TXT', u'XML']
        CFgood = [u'TGZ', u'ZIP']
        OFdict = dict([reversed(pair.split('-')) for pair in doOF.split(',')])
        CFdict = dict([reversed(pair.split('-')) for pair in doCF.split(',')])
        MFdict = dict([reversed(pair.split('-')) for pair in doMF.split(',')])
        OFfound = [OFdict[OFval] for OFval in OFgood if OFval in OFdict]
        if OFfound:
            layerID += OFfound[0]
        else:
            raise AttributeError, 'no acceptable output format found'
        MFfound = [MFdict[MFval] for MFval in MFgood if MFval in MFdict]
        if MFfound:
            layerID += MFfound[0]
        else:
            raise AttributeError, 'no acceptable metadata format found'
        CFfound = [CFdict[CFval] for CFval in CFgood if CFval in CFdict]
        if CFfound:
            layerID += CFfound[0]
        else:
            raise AttributeError, 'no acceptable compression format found'
        return str(layerID)

    def requestvalidation(self, layerIDs):
        """Generates download URLs from layer IDs."""
        retval = {}

        # request validation
        wsdlRequest = "http://extract.cr.usgs.gov/requestValidationService/wsdl/RequestValidationService.wsdl"
        clientRequest = suds.client.Client(wsdlRequest)

        # we now iterate through layerIDs
        for layerID in layerIDs:
            retry = True
            divisions = 1
            layertype = self.layertype(layerID)
            fullLayerID = self.pid2FullPid(layerID) if layerID in self.seamlessIDs else layerID
            downloadURLs = []

            while retry:
                for (xi, yi) in product(xrange(divisions),xrange(divisions)):
                    baseextents = self.wgs84extents[layertype]
                    mapextents = {}
                    mapextents['xmin'] = baseextents['xmin'] + float(xi) * (baseextents['xmax']-baseextents['xmin']) / divisions
                    mapextents['xmax'] = baseextents['xmin'] + float(1 + xi) * (baseextents['xmax']-baseextents['xmin']) / divisions
                    mapextents['ymin'] = baseextents['ymin'] + float(yi) * (baseextents['ymax']-baseextents['ymin']) / divisions
                    mapextents['ymax'] = baseextents['ymin'] + float(1 + yi) * (baseextents['ymax']-baseextents['ymin']) / divisions
                    xmlString = "<REQUEST_SERVICE_INPUT><AOI_GEOMETRY><EXTENT><TOP>%f</TOP>" \
                                "<BOTTOM>%f</BOTTOM><LEFT>%f</LEFT><RIGHT>%f</RIGHT></EXTENT>" \
                                "<SPATIALREFERENCE_WKID/></AOI_GEOMETRY><LAYER_INFORMATION>" \
                                "<LAYER_IDS>%s</LAYER_IDS></LAYER_INFORMATION><CHUNK_SIZE>%d</CHUNK_SIZE>" \
                                "<JSON></JSON></REQUEST_SERVICE_INPUT>" % \
                                (mapextents['ymax'], mapextents['ymin'], mapextents['xmin'], \
                                mapextents['xmax'], fullLayerID, 250) # can be 100, 15, 25, 50, 75, 250
        
                    if layerID in self.seamlessIDs:
                        response = clientRequest.service.processAOI2(xmlString)
                        print("Requested URLs for seamless layer ID %s (aka %s)..." % (layerID, fullLayerID))
                    else:
                        response = clientRequest.service.getTiledDataDirectURLs2(xmlString)
                        print("Requested URLs for tiled layer ID %s..." % layerID)
        
                    # I am still a bad man.
                    downloadURLs += [x.rsplit("</DOWNLOAD_URL>")[0] for x in response.split("<DOWNLOAD_URL>")[1:]]
                    if len(downloadURLs) <= 0:
                        if "<ERROR>" in response and "area of interest" in response:
                            # AOI was too large. Try again.
                            divisions += 1
                            print("Warning: USGS server indicated AOI was too large. Trying again with %d pieces" % (divisions**2))
                            break
                        else: 
                            print("ERROR: No downloadURLs, and unknown error! Response was:\n%s" % response)
                            raise IOError
                    retry = False

            retval[layerID] = downloadURLs

        return retval

    @staticmethod
    def getfn(downloadURL):
        if isinstance(downloadURL, unicode):
            downloadURL = downloadURL.encode('ascii', 'ignore')
        pdURL = urlparse.urlparse(downloadURL)
        pQS = urlparse.parse_qs(pdURL[4])
        if 'mdf' in pQS:
            longfile = "%s%s%s%s%s" % (pQS['key'][0],pQS['lft'][0],pQS['rgt'][0],pQS['top'][0],pQS['bot'][0])
            longfile = longfile.replace('.','')
            justfile = "%s.%s" % (longfile,pQS['arc'][0])
            justfile = justfile.lower()
        else:
            longfile = pQS['FNAME'][0]
            justfile = os.path.split(longfile)[1]
        return justfile

    def retrievefile(self, layerID, downloadURL):
        """Retrieve the datafile associated with the URL.  This may require downloading it from the USGS servers or extracting it from a local archive."""
        print "Attempting to retrieve %s..." % downloadURL
        fname = Region.getfn(downloadURL)
        layerdir = os.path.join(Region.downloadtop, layerID)
        if not os.path.exists(layerdir):
            os.makedirs(layerdir)
        downloadfile = os.path.join(layerdir, fname)

        # Apparently Range checks don't always work across Redirects
        # So find the final URL before starting the download
        if not(layerID in self.seamlessIDs):
            opener = urllib2.build_opener(SmartRedirectHandler())
            dURL = downloadURL
            while True:
                req = urllib2.Request(dURL)
                webPage = opener.open(req)
                if hasattr(webPage, 'status') and webPage.status == 302:
                    dURL = webPage.url
                else:
                    break
            maxSize = int(webPage.headers['Content-Length'])
            webPage.close()

            if os.path.exists(downloadfile):
                outputFile = open(downloadfile, 'ab')
                existSize = os.path.getsize(downloadfile)
                req.headers['Range'] = 'bytes=%s-%s' % (existSize, maxSize)
            else:
                outputFile = open(downloadfile, 'wb')
                existSize = 0
            webPage = opener.open(req)
            if maxSize == existSize:
                print "Using cached file for layerID %s" % layerID
            else:
                print "Downloading file from server for layerID %s" % layerID
                pbar = ProgressBar(widgets=[Percentage(), ' ', Bar(), ' ', ETA(), ' ', FileTransferSpeed()], maxval=maxSize).start()
                # This chunk size may be small!
                max_chunk_size = 8192
                # NB: USGS web servers do not handle resuming downloads correctly
                # so we have to drop incoming data on the floor
                if 'Content-Range' in webPage.headers:
                    # Resuming downloads are now working
                    # We can start from where we left off
                    numBytes = existSize
                else:
                    numBytes = 0
                # if numBytes is less than existSize, do not save what we download
                while numBytes < existSize:
                    pbar.update(numBytes)
                    left = existSize - numBytes
                    chunk_size = left if left < max_chunk_size else max_chunk_size
                    data = webPage.read(chunk_size)
                    numBytes = numBytes + len(data)
                # if numBytes is less than maxSize, save what we download
                while numBytes < maxSize:
                    pbar.update(numBytes)
                    data = webPage.read(max_chunk_size)
                    if not data:
                        break
                    outputFile.write(data)
                    numBytes = numBytes + len(data)
                pbar.finish()
                webPage.close()
                outputFile.close()

        # This code is for Seamless layers. XXX TODO: Doesn't handle partially-fetched files
        else:
            if os.path.exists(downloadfile):
                existSize = os.path.getsize(downloadfile)
            else:
                print("  Requesting download for %s." % layerID)
                # initiateDownload and get the response code
                # put _this_ in its own function!
                try:
                    page = urllib2.urlopen(downloadURL.replace(' ','%20'))
                except IOError, e:
                    if hasattr(e, 'reason'):
                        raise IOError, e.reason
                    elif hasattr(e, 'code'):
                        raise IOError, e.code
                    else:
                        raise IOError
                else:
                    result = page.read()
                    page.close()
                    # parse response for request id
                    if result.find("VALID>false") > -1:
                        # problem with initiateDownload request string
                        # handle that here
                        pass
                    else:
                        # downloadRequest successfully entered into queue
                        startPos = result.find("<ns:return>") + 11
                        endPos = result.find("</ns:return>")
                        requestID = result[startPos:endPos]
                print "  request ID is %s" % requestID
    
                sleep(5)
                while True:
                    dsPage = urllib2.urlopen("http://extract.cr.usgs.gov/axis2/services/DownloadService/getDownloadStatus?downloadID=%s" % requestID)
                    result = dsPage.read()
                    dsPage.close()
                    result = result.replace("&#xd;\n"," ")
                    # parse out status code and status text
                    startPos = result.find("<ns:return>") + 11
                    endPos = result.find("</ns:return>")
                    (code, status) = result[startPos:endPos].split(',', 1)
                    print "  status is %s" % status
                    if (int(code) == 400):
                        break
                    sleep(15)
    
                getFileURL = "http://extract.cr.usgs.gov/axis2/services/DownloadService/getData?downloadID=%s" % requestID
                try:
                    page3 = urllib2.Request(getFileURL)
                    opener = urllib2.build_opener(SmartRedirectHandler())
                    obj = opener.open(page3)
                    location = obj.headers['Location'] 
                    #filename = location.split('/')[-1].split('#')[0].split('?')[0]        
                except IOError, e:
                    if hasattr(e, 'reason'):
                        raise IOError, e.reason
                    elif hasattr(e, 'code'):
                        raise IOError, e.code
                    else:
                        raise IOError
                else:
                    print "  downloading %s now!" % downloadfile
                    downloadFile = open(downloadfile, 'wb')
                    while True:
                        data = obj.read(8192)
                        if data == "":
                            break
                        downloadFile.write(data)
                    downloadFile.close()
                    obj.close()
    
                # UGH
                setStatusURL = "http://extract.cr.usgs.gov/axis2/services/DownloadService/setDownloadComplete?downloadID=%s" % requestID
                try:
                    page4 = urllib2.urlopen(setStatusURL)
                except IOError, e:
                    if hasattr(e, 'reason'):
                        raise IOError, e.reason
                    elif hasattr(e, 'code'):
                        raise IOError, e.code
                    else:
                        raise IOError
                else:
                    result = page4.read()
                    page4.close()
                    # remove carriage returns
                    result = result.replace("&#xd;\n"," ")
                    # parse out status code and status text
                    startPos = result.find("<ns:return>") + 11
                    endPos = result.find("</ns:return>")
                    status = result[startPos:endPos]

        # FIXME: this is grotesque
        extracthead = fname.split('.')[0]
        layertype = self.layertype(layerID)
        if layertype == 'elevation':
            extractfiles = [os.path.join(extracthead, '.'.join(['float%s_%s' % (extracthead, Region.exsuf[layerID]), suffix])) for suffix in 'flt', 'hdr', 'prj']
        elif layertype == 'ortho':
            extractfiles = ['.'.join([extracthead, suffix]) for suffix in ['tif']]
        else: # if layertype == 'landcover':
            extractfiles = ['.'.join([extracthead, suffix]) for suffix in 'tif', 'tfw']
        for extractfile in extractfiles:
            if os.path.exists(os.path.join(layerdir, extractfile)):
                print "Using existing file %s for layerID %s" % (extractfile, layerID)
            else:
                if downloadfile[-3:] == 'zip':
                    os.system('unzip "%s" "%s" -d "%s"' % (downloadfile, extractfile, layerdir))
                elif downloadfile[-3:] == 'tgz' or downloadfile[-5:] == 'tar.gz':
                    tf = tarfile.open(downloadfile,'r')
                    for fn in tf.getnames():
                        if fn[-3:] == 'tif':
                            tf.close()
                            print('cd %s && tar --strip-components 1 -xzvf "%s" "%s"' % (layerdir, downloadfile, fn))
                            os.system('tar --strip-components 1 -xzvf "%s" "%s" -C "%s"' % (downloadfile, fn, layerdir))
                            os.system('mv %s %s/%s.tif' % (fn.split('/')[-1], layerdir, fname.split('.')[0]))
                            break
                else:
                    raise IOError
        return os.path.join(layerdir, extractfiles[0])

    def getfiles(self):
        """Get files from USGS and extract them if necessary."""
        layerIDs = [self.oilayer, self.lclayer, self.ellayer]
        downloadURLs = self.requestvalidation(layerIDs)
        for layerID in downloadURLs.keys():
            print "Downloading files for layer %s..." % layerID
            extractlist = []
            for downloadURL in downloadURLs[layerID]:
                extractfile = self.retrievefile(layerID, downloadURL)
                extractlist.append(extractfile)
            if len(extractlist) <= 0:
                print("ERROR: Zero files found for layer %s!\n" % layerID)
                raise IOError
            # Build VRTs
            print "Building VRT file for layer %s (this may take some time)..." % layerID
            vrtfile = os.path.join(self.mapsdir, '%s.vrt' % layerID)
            buildvrtcmd = 'gdalbuildvrt "%s" %s' % (vrtfile, ' '.join(['"%s"' % os.path.abspath(extractfile) for extractfile in extractlist]))
            print "$ "+buildvrtcmd
            os.system('%s' % buildvrtcmd)
            # Generate warped GeoTIFFs
            tiffile = os.path.join(self.mapsdir, '%s.tif' % layerID)
            warpcmd = 'gdalwarp -q -multi -t_srs "%s" "%s" "%s"' % (Region.t_srs, vrtfile, tiffile)
            os.system('%s' % warpcmd)

    def buildmap(self, wantCL=True):
        """Use downloaded files and other parameters to build multi-raster map."""

        # warp elevation data into new format
        # NB: can't do this to landcover until mode algorithm is supported
        print "First-pass warp and store over elevation data..."
        eltif = os.path.join(self.mapsdir, '%s.tif' % (self.ellayer)) 
        elfile = os.path.join(self.mapsdir, '%s-new.tif' % (self.ellayer))
        elextents = self.albersextents['elevation']
        warpcmd = 'gdalwarp -q -multi -t_srs "%s" -tr %d %d -te %d %d %d %d' \
                  ' -r cubic "%s" "%s" -srcnodata "-340282346638529993179660072199368212480.000" ' \
                  '-dstnodata 0' % (Region.t_srs, self.scale, self.scale, \
                  elextents['xmin'], elextents['ymin'], elextents['xmax'], \
                  elextents['ymax'], eltif, elfile)

        try:
            os.remove(elfile)
        except OSError:
            pass
        # NB: make this work on Windows too!
        os.system('%s' % warpcmd)
        print "Completed first-pass warp over elevation data."

        # Warp ortho data into new format
        print "First-pass warp and store over orthoimagery data..."
        oitif = os.path.join(self.mapsdir, '%s.tif' % (self.oilayer)) 
        oifile = os.path.join(self.mapsdir, '%s-new.tif' % (self.oilayer))
        oiextents = self.albersextents['ortho']
        warpcmd = 'gdalwarp -q -multi -t_srs "%s" -tr %d %d -te %d %d %d %d' \
                  ' -r cubic "%s" "%s" -srcnodata "-340282346638529993179660072199368212480.000" ' \
                  '-dstnodata 0' % (Region.t_srs, self.scale, self.scale, \
                  oiextents['xmin'], oiextents['ymin'], oiextents['xmax'], \
                  oiextents['ymax'], oitif, oifile)
  
        try:
            os.remove(oifile)
        except OSError:
            pass
        # NB: make this work on Windows too!
        os.system('%s' % warpcmd)
        print "Completed first-pass warp over orthoimagery data."
    
        # First compute global elevation data
        elds = gdal.Open(elfile, GA_ReadOnly)
        elgeoxform = elds.GetGeoTransform()
        elband = elds.GetRasterBand(1)
        (elmin, elmax) = elband.ComputeRasterMinMax(False)
        elmin = int(elmin)
        elmax = int(elmax)
        elxsize = elds.RasterXSize
        elysize = elds.RasterYSize
        elband = None
        elds = None

        # sealevel depends upon elmin
        minsealevel = 2
        # if minimum elevation is below sea level, add extra space
        if (elmin < 0):
            minsealevel += int(-1.0*elmin/self.scale)
        maxsealevel = Region.tileheight - Region.headroom
        oldsealevel = self.sealevel
        if (oldsealevel > maxsealevel or oldsealevel < minsealevel):
            print "warning: sealevel value %d outside %d-%d range" % (oldsealevel, minsealevel, maxsealevel)
        self.sealevel = int(min(max(oldsealevel, minsealevel), maxsealevel))

        # maxdepth depends upon sealevel
        minmaxdepth = 1
        maxmaxdepth = self.sealevel - 1
        oldmaxdepth = self.maxdepth
        if (oldmaxdepth > maxmaxdepth or oldmaxdepth < minmaxdepth):
            print "warning: maxdepth value %d outside %d-%d range" % (oldmaxdepth, minmaxdepth, maxmaxdepth)
        self.maxdepth = int(min(max(oldmaxdepth, minmaxdepth), maxmaxdepth))

        # trim depends upon elmin (if elmin < 0, trim == 0)
        mintrim = Region.trim
        maxtrim = max(elmin, mintrim)
        oldtrim = self.trim
        if (oldtrim > maxtrim or oldtrim < mintrim):
            print "warning: trim value %d outside %d-%d range" % (oldtrim, mintrim, maxtrim)
        self.trim = int(min(max(oldtrim, mintrim), maxtrim))

        # vscale depends on sealevel, trim and elmax
        # NB: no maximum vscale, the sky's the limit (hah!)
        eltrimmed = elmax - self.trim
        elroom = Region.tileheight - Region.headroom - self.sealevel
        minvscale = ceil(eltrimmed / elroom)
        oldvscale = self.vscale
        if (oldvscale < minvscale):
            print "warning: vscale value %d smaller than minimum value %d" % (oldvscale, minvscale)
        self.vscale = int(max(oldvscale, minvscale))
        # vscale/trim/maxdepth computation ends here

        srs = osr.SpatialReference()
        srs.ImportFromProj4(Region.t_srs)
        driver = gdal.GetDriverByName("GTiff")

        lctif = os.path.join(self.mapsdir, '%s.tif' % (self.lclayer)) 
        tifds = gdal.Open(lctif, GA_ReadOnly)
        tifgeotrans = tifds.GetGeoTransform()
        tifds = None
        lcextentsWhole = self.albersextents['landcover']
        xminarr = (lcextentsWhole['xmin']-tifgeotrans[0])/tifgeotrans[1]
        xmaxarr = (lcextentsWhole['xmax']-tifgeotrans[0])/tifgeotrans[1]
        yminarr = (lcextentsWhole['ymax']-tifgeotrans[3])/tifgeotrans[5]
        ymaxarr = (lcextentsWhole['ymin']-tifgeotrans[3])/tifgeotrans[5]
        print("Landcover extents are %s -> %s" % (str(lcextentsWhole), str([xminarr, xmaxarr, yminarr, ymaxarr])))

        tiftiles = []

        print("Processing %d x %d data array as tiles." % (elxsize, elysize))
        for (tilex, tiley) in product(xrange(int(ceil(float(elxsize)/Region.prepTileSize))), \
                                      xrange(int(ceil(float(elysize)/Region.prepTileSize)))):

            offsetx = tilex*Region.prepTileSize
            offsety = tiley*Region.prepTileSize
            sizex   = min(Region.prepTileSizeOverlap, elxsize - offsetx)
            sizey   = min(Region.prepTileSizeOverlap, elysize - offsety)

            # GeoTIFF Image Tile
            # eight bands: landcover, elevation, bathy, crust, oR, oG, oB, oA
            # data type is GDT_Int16 (elevation can be negative)
            print("Creating output image tile %d, %d with offset (%d, %d) and size (%d, %d)" % \
                  (tilex, tiley, offsetx, offsety, sizex, sizey))
            tilename = os.path.join(self.regiondir, 'Tile_%04d_%04d.tif' % (tilex, tiley))
            mapds = driver.Create(tilename, sizex, sizey, len(Region.rasters), GDT_Int16)
            mapds.SetProjection(srs.ExportToWkt())
            # overall map transform should match elevation map transform
            geoxform = [elgeoxform[0] + offsetx * elgeoxform[1], elgeoxform[1], elgeoxform[2], \
                        elgeoxform[3] + offsety * elgeoxform[5], elgeoxform[4], elgeoxform[5]]
            mapds.SetGeoTransform(geoxform)

            # modify elarray and save it as raster band 2
            print "Adjusting and storing elevation to GeoTIFF..."
            elds = gdal.Open(elfile, GA_ReadOnly)
            elarray = elds.GetRasterBand(1).ReadAsArray(offsetx, offsety, sizex, sizey)
            elds = None
            actualel = ((elarray - self.trim)/self.vscale)+self.sealevel
            mapds.GetRasterBand(Region.rasters['elevation']).WriteArray(actualel)
            elarray = None
            actualel = None

            # generate crust and save it as raster band 4
            print "Adjusting and storing crust to GeoTIFF..."
            newcrust = Crust(sizex, sizey, wantCL=wantCL)
            crustarray = newcrust()
            mapds.GetRasterBand(Region.rasters['crust']).WriteArray(crustarray)
            crustarray = None
            newcrust = None
    
            # Compute new geographic tile extents for landcover
            lcextents = {}
            border   = self.scale * self.maxdepth
            lcxsize  = elxsize + 2 * border   # Total LC region size
            lcysize  = elysize + 2 * border
            lctxsize = sizex + 2 * border     # Current LC tile size
            lctysize = sizey + 2 * border
            lcextents['xmin'] = lcextentsWhole['xmin'] + \
                                (offsetx / float(lcxsize)) * \
                                (lcextentsWhole['xmax'] - lcextentsWhole['xmin'])
            lcextents['xmax'] = lcextentsWhole['xmin'] + \
                                ((offsetx + lctxsize) / float(lcxsize)) * \
                                (lcextentsWhole['xmax'] - lcextentsWhole['xmin'])
            lcextents['ymax'] = lcextentsWhole['ymax'] - \
                                (offsety / float(lcysize)) * \
                                (lcextentsWhole['ymax'] - lcextentsWhole['ymin'])
            lcextents['ymin'] = lcextentsWhole['ymax'] - \
                                ((offsety + lctysize) / float(lcysize)) * \
                                (lcextentsWhole['ymax'] - lcextentsWhole['ymin'])

            # Compute new array tile extents for landcover
            lcxminarr = int(xminarr + (offsetx / float(lcxsize)) * (xmaxarr - xminarr))
            lcxmaxarr = int(xminarr + ((offsetx + lctxsize) / float(lcxsize)) * (xmaxarr - xminarr))
            lcyminarr = int(yminarr + (offsety / float(lcysize)) * (ymaxarr - yminarr))
            lcymaxarr = int(yminarr + ((offsety + lctysize) / float(lcysize)) * (ymaxarr - yminarr))

            # read landcover array
            print "Warping and storing landcover and bathy data..."
            print("LC extents: %s" % str(lcextents))
            print("LC window: %s => %s" % (str([xminarr, xmaxarr, yminarr, ymaxarr]), str([lcxminarr, lcyminarr, lcxmaxarr-lcxminarr, lcymaxarr-lcyminarr])))
            tifds = gdal.Open(lctif, GA_ReadOnly)
            tifband = tifds.GetRasterBand(1)
            values = tifband.ReadAsArray(lcxminarr, lcyminarr, lcxmaxarr-lcxminarr, lcymaxarr-lcyminarr)
            tifnodata = tifband.GetNoDataValue()  #nodata is treated as water, which is 11
            if (tifnodata == None):
                tifnodata = 0
            values[values == tifnodata] = 11
            values = values.flatten()
            tifband = None
            tifds = None

            # 2. a new array of original scale coordinates must be created
            #    The following two lines should work fine still because x and y are offset into the array
            tifxrange = [tifgeotrans[0] + tifgeotrans[1] * x for x in xrange(lcxminarr, lcxmaxarr)]
            tifyrange = [tifgeotrans[3] + tifgeotrans[5] * y for y in xrange(lcyminarr, lcymaxarr)]
            coords = numpy.array([(x, y) for y in tifyrange for x in tifxrange])

            # 3. a new array of goal scale coordinates must be made
            # landcover extents are used for the bathy depth array
            # yes, it's confusing.  sorry.
            depthxlen = int((lcextents['xmax']-lcextents['xmin'])/self.scale)
            depthylen = int((lcextents['ymax']-lcextents['ymin'])/self.scale)
            depthxrange = [lcextents['xmin'] + self.scale * x for x in xrange(depthxlen)]
            depthyrange = [lcextents['ymax'] - self.scale * y for y in xrange(depthylen)]
            depthbase = numpy.array([(x, y) for y in depthyrange for x in depthxrange])

            # 4. an inverse distance tree must be built from that
            lcCLIDT = CLIDT(coords, values, depthbase, wantCL=wantCL)

            # 5. the desired output comes from that inverse distance tree
            deptharray = lcCLIDT()
            deptharray.resize((depthylen, depthxlen))
            lcCLIDT = None

            # 6. Finish and store the band
            lcarray = deptharray[self.maxdepth:-1*self.maxdepth, self.maxdepth:-1*self.maxdepth]
            geotrans = [ lcextents['xmin'], self.scale, 0, lcextents['ymax'], 0, -1 * self.scale ]
            projection = srs.ExportToWkt()
            bathyarray = getBathy(deptharray, self.maxdepth, geotrans, projection)
            mapds.GetRasterBand(Region.rasters['bathy']).WriteArray(bathyarray)
    
            # perform terrain translation
            # NB: figure out why this doesn't work up above
            lcpid = self.lclayer[:3]
            if lcpid in Terrain.translate:
                trans = Terrain.translate[lcpid]
                for key in trans:
                    lcarray[lcarray == key] = trans[key]
                for value in numpy.unique(lcarray).flat:
                    if value not in Terrain.terdict:
                        print "bad value: ", value
            print lcarray.shape
            mapds.GetRasterBand(Region.rasters['landcover']).WriteArray(lcarray)
    
            # read and store ortho arrays
            tifds = gdal.Open(oifile, GA_ReadOnly)
            for band in xrange(1,5):		# That is, [1 2 3 4]
                print "Cropping and storing ortho data (%s band)..." % "-rgba"[band]
                tifband = tifds.GetRasterBand(band)
                #values = tifband.ReadAsArray(oixminarr, oiyminarr, (oixmaxarr - oixminarr), (oiymaxarr - oiyminarr))
                values = tifband.ReadAsArray(offsetx, offsety, sizex, sizey)
                tifnodata = tifband.GetNoDataValue()
                if (tifnodata == None):
                    tifnodata = 0
                values[values == tifnodata] = -1    #Signal missing
                tifband = None
    
                print values.shape
                mapds.GetRasterBand(band+Region.rasters['orthor']-1).WriteArray(values)
                values = None
            tifds = None

            # close the dataset and append it to the tile list
            mapds = None
            tiftiles.append(tilename)

        # Now join everything together!
        print("Joining %d files into final TIF image..." % len(tiftiles))
        mergecmd = 'gdal_merge.py -o "%s" "%s"' % (self.mapname, '" "'.join(tiftiles))
  
        try:
            os.remove(self.mapname)
        except OSError:
            pass
        # NB: make this work on Windows too!
        os.system('%s' % mergecmd)

        print("Merge complete! Removing tiles...")
        [os.remove(tilename) for tilename in tiftiles]
        print("Cleanup complete.")

    # Thanks to http://williams.best.vwh.net/avform.htm#LL
    # Takes a base (lat1, lon1) point and offsets by dist meters in the angle direction
    def lloffset(self, lat1, lon1, angle, dist):
        lat1 = math.radians(lat1)
        lon1 = math.radians(lon1)
        a = 6378137.0 # Equatorial radius, meters
        b = 6356752.3 # Polar radius, meters
        earth_rad = math.sqrt( ((((a ** 2)*math.cos(lat1)) ** 2) + (((b ** 2)*math.sin(lat1)) ** 2))
                             / (((a*math.cos(lat1)) ** 2) + ((b*math.sin(lat1)) ** 2)))
        print("earth rad %f at lat %f lon %f" % (earth_rad, lat1, lon1))
        dist /= earth_rad
        lat = math.asin(math.sin(lat1) * math.cos(dist) + \
                        math.cos(lat1) * math.sin(dist) * math.cos(angle))
        lon = lon1 if math.cos(lat) == 0 else \
              math.fmod(lon1-math.asin(math.sin(angle) * math.sin(dist) / math.cos(lat)) + math.pi, 2 * math.pi) - math.pi
        return (math.degrees(lat), math.degrees(lon))

