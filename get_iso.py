import os
import urllib
from thredds_crawler.crawl import Crawl

import logging
import logging.handlers
logger = logging.getLogger('thredds_crawler')
fh = logging.handlers.RotatingFileHandler('/home/testbed/iso/logs/iso_harvest.log', maxBytes=1024*1024*10, backupCount=5)
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

SAVE_DIR="/home/testbed/iso/iso_records"

THREDDS_SERVERS = {
    "espresso1": "http://tds.marine.rutgers.edu/thredds/roms/espresso/2009_da/catalog.html",
    "espresso2": "http://tds.marine.rutgers.edu/thredds/catalog/roms/espresso/2013_da/avg_Best/catalog.html",
    "espresso3": "http://tds.marine.rutgers.edu/thredds/catalog/roms/espresso/2013_da/his_Best/catalog.html",
    "smast2": "http://www.smast.umassd.edu:8080/thredds/hindcasts.html",
    "smast3": "http://www.smast.umassd.edu:8080/thredds/archives.html",
    "comt1":   "http://comt.sura.org/thredds/comt_1_archive_summary.html",
    "comt2":   "http://comt.sura.org/thredds/comt_2_current.html",
    "useast":  "http://omgsrv1.meas.ncsu.edu:8080/thredds/catalog/fmrc/us_east/catalog.html",
    "wfs_roms_nf": "http://crow.marine.usf.edu:8080/thredds/catalog/WFS_ROMS_NF_model/catalog.html",
    "wfs_fvcom": "http://crow.marine.usf.edu:8080/thredds/fvcom_agg.html",
    "wfs_swan": "http://crow.marine.usf.edu:8080/thredds/swan_agg.html",
    "tampa_hf_radar": "http://crow.marine.usf.edu:8080/thredds/hf_radar_agg.html",
    "hycom_region1": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/hycom/hycom_reg1_agg/catalog.html",
    "hycom_region6": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/hycom/hycom_reg6_agg/catalog.html",
    "hycom_region7": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/hycom/hycom_reg7_agg/catalog.html",
    "hycom_region17": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/hycom/hycom_reg17_agg/catalog.html",
    '1': u'http://geoport.whoi.edu/thredds/catalog/coawst_4/use/fmrc/catalog.xml',
    '10': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg10_agg/catalog.xml',
     '11': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg1_agg/catalog.xml',
     '12': u'http://ona.coas.oregonstate.edu:8080/thredds/catalog.xml',
     '16': u'http://www.smast.umassd.edu:8080/thredds/forecasts.xml',
     '17': u'http://geoport-dev.whoi.edu/thredds/estofs_agg.xml',
     '18': u'http://colossus.dl.stevens-tech.edu/thredds/complete_latest.xml',
     '19': u'http://www.cencoos.org/thredds/catalog/gliders/bloomex/catalog.xml',
     '31': u'http://omgsrv1.meas.ncsu.edu:8080/thredds/catalog/fmrc/sabgom/catalog.xml',
     '32': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_sfc8_hind_agg/catalog.xml',
     '33': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_us_east_before_depth_change_agg/catalog.xml',
     '34': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_useast_agg_20091119_20130404/catalog.xml',
     '35': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_us_east_agg/catalog.xml',
     '36': u' http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_amseas_agg_20091119_20130404/catalog.xml',
     '37': u' http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_amseas_agg/catalog.xml',
     '38': u'http://hfrnet.ucsd.edu/thredds/HFRADAR_USEGC_hourly_GNOME.xml',
     '39': u'http://hfrnet.ucsd.edu/thredds/HFRADAR_USHI_hourly_GNOME.xml',
     '4': u'http://sos.maracoos.org/thredds/catalog.xml',
     '40': u'http://hfrnet.ucsd.edu/thredds/HFRADAR_PRVI_hourly_GNOME.xml',
     '43': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/GBOFS/fmrc/catalog.xml',
     '44': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/CBOFS/fmrc/catalog.xml',
     '45': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/LOOFS/fmrc/catalog.xml',
     '46': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/LMOFS/fmrc/catalog.xml',
     '47': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/LEOFS/fmrc/catalog.xml',
     '48': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/LSOFS/fmrc/catalog.xml',
     '49': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/SJROFS/fmrc/catalog.xml',
     '5': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg7_agg/catalog.xml',
     '50': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/TBOFS/fmrc/catalog.xml',
     '6': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_sfc8_agg/catalog.xml',
     '7': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg6_agg/catalog.xml',
     '8': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg2_agg/catalog.xml',
     '9': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg5_agg/catalog.xml',
     '94': u'http://omgsrv1.meas.ncsu.edu:8080/thredds/catalog/fmrc/us_east/catalog.xml',
     '95': u'http://dm2.caricoos.org/thredds/catalog/swan/catalog.xml' 
}

for subfolder, thredds_url in THREDDS_SERVERS.items():
  logger.info("Crawling %s (%s)" % (subfolder, thredds_url))
  crawler = Crawl(thredds_url, debug=True)
  isos = [(d.id, s.get("url")) for d in crawler.datasets for s in d.services if s.get("service").lower() == "iso"]
  filefolder = os.path.join(SAVE_DIR, subfolder)
  if not os.path.exists(filefolder):
    os.makedirs(filefolder)
  for iso in isos:
    try:
      filename = iso[0].replace("/", "_") + ".iso.xml"
      filepath = os.path.join(filefolder, filename)
      logger.info("Downloading/Saving %s" % filepath)
      urllib.urlretrieve(iso[1], filepath)
    except BaseException:
      logger.exception("Error!")
