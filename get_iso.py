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
    "smast1": "http://www.smast.umassd.edu:8080/thredds/forecasts.html",
    "smast2": "http://www.smast.umassd.edu:8080/thredds/hindcasts.html",
    "smast3": "http://www.smast.umassd.edu:8080/thredds/archives.html",
    "comt1":      "http://comt.sura.org/thredds/comt_1_archive_summary.html",
    "comt2":   "http://comt.sura.org/thredds/comt_2_current.html",
    "coawst":   "http://geoport.whoi.edu/thredds/catalog/coawst_4/use/fmrc/catalog.html", 
    "sabgom":  "http://omgsrv1.meas.ncsu.edu:8080/thredds/catalog/fmrc/sabgom/catalog.html", 
    "useast":  "http://omgsrv1.meas.ncsu.edu:8080/thredds/catalog/fmrc/us_east/catalog.html",
    "wfs_roms_nf": "http://crow.marine.usf.edu:8080/thredds/catalog/WFS_ROMS_NF_model/catalog.html",
    "wfs_fvcom": "http://crow.marine.usf.edu:8080/thredds/fvcom_agg.html",
    "wfs_swan": "http://crow.marine.usf.edu:8080/thredds/swan_agg.html",
    "tampa_hf_radar": "http://crow.marine.usf.edu:8080/thredds/hf_radar_agg.html",
    "ncom_region1": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg1_agg/catalog.html",
    "hycom_region1": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/hycom/hycom_reg1_agg/catalog.html",
    "amseas_catalog1": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_amseas_agg/catalog.html",
    "amseas_catalog2": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_amseas_agg_20091119_20130404/catalog.html",
    "ncom_us_east3": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_us_east_agg/catalog.html",
    "ncom_us_east2": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_useast_agg_20091119_20130404/catalog.html",
    "ncom_us_east1": "http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom_us_east_agg/catalog.html" 
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
