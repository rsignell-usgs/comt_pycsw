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
    '0': u'http://www.neracoos.org/thredds/WW3.xml',
    '1': u'http://geoport.whoi.edu/thredds/catalog/coawst_4/use/fmrc/catalog.xml',
    '10': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg10_agg/catalog.xml',
     '11': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg1_agg/catalog.xml',
     '12': u'http://ona.coas.oregonstate.edu:8080/thredds/catalog.xml',
     '13': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/028p1/catalog.xml',
     '14': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/128p1/catalog.xml',
     '15': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/133p1/catalog.xml',
     '16': u'http://www.smast.umassd.edu:8080/thredds/forecasts.xml',
     '17': u'http://geoport-dev.whoi.edu/thredds/estofs_agg.xml',
     '18': u'http://colossus.dl.stevens-tech.edu/thredds/complete_latest.xml',
     '19': u'http://www.cencoos.org/thredds/catalog/gliders/bloomex/catalog.xml',
     '2': u'http://sos.maracoos.org/stable/agg_catalogs/weatherflow_agg_catalog.xml',
     '20': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/029p1/catalog.xml',
     '21': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/036p1/catalog.xml',
     '22': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/071p1/catalog.xml',
    '23': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/076p1/catalog.xml',
    '24': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/081p1/catalog.xml',
    '25': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/087p1/catalog.xml',
     '26': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/088p1/catalog.xml',
     '27': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/089p1/catalog.xml',
     '28': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/090p1/catalog.xml',
     '29': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/091p1/catalog.xml',
     '3': u'http://sos.maracoos.org/stable/agg_catalogs/sldmb_agg_catalog.xml',
     '30': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/095p1/catalog.xml',
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
     '41': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/104p1/catalog.xml',
     '42': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/105p1/catalog.xml',
     '43': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/GBOFS/fmrc/catalog.xml',
     '44': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/CBOFS/fmrc/catalog.xml',
     '45': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/LOOFS/fmrc/catalog.xml',
     '46': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/LMOFS/fmrc/catalog.xml',
     '47': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/LEOFS/fmrc/catalog.xml',
     '48': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/LSOFS/fmrc/catalog.xml',
     '49': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/SJROFS/fmrc/catalog.xml',
     '5': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg7_agg/catalog.xml',
     '50': u'http://opendap.co-ops.nos.noaa.gov/thredds/catalog/TBOFS/fmrc/catalog.xml',
     '51': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/139p1/catalog.xml',
     '52': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/143p1/catalog.xml',
     '53': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/144p1/catalog.xml',
     '54': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/151p1/catalog.xml',
     '55': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/152p1/catalog.xml',
     '56': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/154p1/catalog.xml',
     '57': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/163p1/catalog.xml',
     '58': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/164p1/catalog.xml',
     '59': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/165p1/catalog.xml',
     '6': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_sfc8_agg/catalog.xml',
     '60': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/166p1/catalog.xml',
     '61': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/167p1/catalog.xml',
     '62': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/168p1/catalog.xml',
     '63': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/169p1/catalog.xml',
     '64': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/170p1/catalog.xml',
     '65': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/172p1/catalog.xml',
     '66': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/174p1/catalog.xml',
     '67': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/175p1/catalog.xml',
     '68': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/176p1/catalog.xml',
     '69': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/179p1/catalog.xml',
     '7': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg6_agg/catalog.xml',
     '70': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/180p1/catalog.xml',
     '71': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/181p1/catalog.xml',
     '72': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/182p1/catalog.xml',
     '73': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/184p1/catalog.xml',
     '74': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/185p1/catalog.xml',
     '75': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/186p1/catalog.xml',
     '76': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/187p1/catalog.xml',
     '77': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/188p1/catalog.xml',
     '78': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/190p1/catalog.xml',
     '79': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/191p1/catalog.xml',
     '8': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg2_agg/catalog.xml',
     '80': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/192p1/catalog.xml',
     '81': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/195p1/catalog.xml',
     '82': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/196p1/catalog.xml',
     '83': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/197p1/catalog.xml',
     '84': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/198p1/catalog.xml',
     '85': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/200p1/catalog.xml',
     '86': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/201p1/catalog.xml',
     '87': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/203p1/catalog.xml',
     '88': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/204p1/catalog.xml',
     '89': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/205p1/catalog.xml',
     '9': u'http://ecowatch.ncddc.noaa.gov/thredds/catalog/ncom/ncom_reg5_agg/catalog.xml',
     '90': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/207p1/catalog.xml',
     '91': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/430p1/catalog.xml',
     '92': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/431p1/catalog.xml',
     '93': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/432p1/catalog.xml',
     '94': u'http://omgsrv1.meas.ncsu.edu:8080/thredds/catalog/fmrc/us_east/catalog.xml',
     '95': u'http://dm2.caricoos.org/thredds/catalog/swan/catalog.xml',
     '96': u'http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/archive/043p1/catalog.xml'
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
