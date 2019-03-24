# -*- coding: cp1255 -*-
#!! -*- coding: utf-8 -*-
"""
Created on 22/03/19

@author: Symbiomatrix

Todo:
- Generate data: Rudimentary, id + status + date (custom), movement (chisq time).
- Pandas utils module, including class for special functions.
- Status merge trick.
- Movement fill trick.
- Export to csv + json.

Features:


Future:
 

Notes:

Bugs:

Version log:
22/03/19 V0.1 General structure initiated. 
22/03/19 V0.0 New.

"""

#BM! Devmode
DEVMODE = True
if DEVMODE: # DEV
#     import Lite_CC_Exp # Using "import as" in optional clause shows warning in eclipse.
#     ldb = Lite_CC_Exp
    BRANCH = "Dev\\"
    LOGFLD = BRANCH + "errlog.log"
    INIFLD = BRANCH + "RTMain"
    DBFLD = BRANCH + "DBFiles"
    BADLOG = BRANCH + "Devlog-d{}.txt" 
    LOADDUM = True # Load from dum function or web. # TEMP
    RUNSELEN = True # Force selena load.
    DUMN1 = 101 # List dum.
    NAVN1 = 7220184 # Replaces pend conds, use a real one (from nav).
    TSTCOL = 4 # For a test query.
    TSTWROW = 1 
else: # PROD
#     import Lite_CC
#     ldb = Lite_CC
    BRANCH = "Prd\\"
    LOGFLD = BRANCH + "errlog.log"
    INIFLD = BRANCH + "CCMain"
    DBFLD = BRANCH + "DBFiles" # Cannot use none because dbset is fixed. 
    BADLOG = BRANCH + "Logfail-d{}.txt"
    LOADDUM = False
    RUNSELEN = True # Preferable conduct to prevent detection.
    TSTCOL = 1
    TSTWROW = 1


# BM! Imports
import sys
import os
#sys.stdout = open('PrintRedir.txt', 'w')
import logging
logger = logging.getLogger(__name__)
if __name__ == '__main__':
    # When seeking all module logs, change bsconf to debug.
    # logging.basicConfig(stream=sys.stderr,
    #                     level=logging.ERROR, # Prints to the console.
    #                     format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
    logging.basicConfig(filename=LOGFLD,
                        level=logging.WARN, # Prints to a file. Should be utf. #.warn
                        format="%(asctime)s %(levelname)s:%(name)s:%(message)s") # Adds timestamp.
    logger.setLevel(logging.DEBUG)
SYSENC = "cp1255" # System default encoding for safe logging.

import utils as uti
# import Design_FT # Main - once interfaces are added.
# import Design2_FT # Query.
import time
import datetime
from pathlib import Path # v
from itertools import product # v
import hashlib
import pandas as pd
import numpy as np

import inspect
from collections import OrderedDict # v

# import requests
# from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
# from selenium.common.exceptions import WebDriverException
# from PyQt5 import QtCore, QtGui, QtWidgets # v

# BM! Constants
FMAIN = 1 # 1 = Location list. 2 = Employee range list. 3 = Unfiltered.
Deb_Prtlog = lambda x,y = logging.ERROR:uti.Deb_Prtlog(x,y,logger)
LSTPH = uti.LSTPH
SECONDS = 1
MINUTES = 60 * SECONDS
HOURS = 60 * MINUTES
DAYS = 24 * HOURS
BCKSLS = "\\"
SLSH = "/"
APOS = "\'"
QUOS = "\""
NLN = "\n"
SPRTXT = """char(13) || '---' || char(13)"""
#QUOTSCP = (r"""‘’‚“”„""","""\'\'\'\"\"\"""")
HASHDEF = 8 # Number of digits.
DEFDT = datetime.datetime(1971,1,1)

LOGMSG = uti.LOGMSG
LOGMSG.update({
"null": "This is the end my friend."
})
METHCODES = uti.METHCODES
METHCODES.update({
"ok": (0,""),
})
REVMETHCD = {v[0]: (k,v[1]) for (k,v) in METHCODES.items()}

# BM! Defs

def DebP_Curbod(curbod,htmlbod,windlen = 30):
    """Short segment for print around list markers in long text.
     
    For debug."""
    for i,_ in enumerate(curbod):
        if curbod[i] != 0:
            st = max(curbod[i] - round(windlen / 2),0)
            ed = curbod[i] + round(windlen / 2)
            print(i,curbod[i],": ",repr(htmlbod[st:ed]))
            
def DebP_Select(seldb,ftc = 1000):
    """Fetches in iterations, prints to console with timers in log.
    
    Spam."""
    scur = "DO"
    pwid = None
    i = 0
    while scur is not None: # Make sure scur is sent back, otherwise generating inf loop.
        uti.St_Timer("Dbprint")
        if len(scur) == 2: # Comparison is bothersome.
            scur = None
        (lrecs,scur,heads) = seldb.Select_Recs(ftc,oldrun = scur)
        if pwid is not None: # Start indicator.
            heads = None
        edind = scur is None # No need for additional ind.
        pwid = seldb.Print_Sel(lrecs,heads,edind,pstwid = pwid)
        i = i + 1
        tdiff = uti.Ed_Timer("Dbprint")
        logger.debug("Batch {} time: {}".format(i,tdiff))
        
def Generate_Samples(vtyp = 1,**parms):
    """Creates a frame of ids, statuses or vitals.
    
    Ids = 1, stat = 2, vit = 3.
    For stat / vit, send ids for which to generate data,
    and optionally dates."""
    if vtyp == 1:
        df = pd.DataFrame()

# BM! MAIN()
def Main_Test():
    """Short activation.
    
    Spam."""
    global galena
    Deb_Prtlog("Start main TEST",logging.DEBUG)
    uti.St_Timer("Main")
    
#     dbrefs = Init_Parms()
#     (cmpdb,infdb,dbini) = dbrefs
    indstop = False
    verr = 0
    btc = 100 # dbini["Main"][INIBTC]
#     ddl = ldb.Date_Conv(dbini["Main"][INIDDL])
#    from Rawdt_LDL import lwin
#    lfail = [("Crystal_Babyface-2000-11-27.jpg","19-Sep-2001 19:44 ","7.9K"),
#             ("Crystal_Babyface.jpg","19-Sep-2001 19:44 ","7.9K"),
#             ("abreik_meerca-.jpg","01-Apr-2018 22:43 ","65K")]
#    tstdl = "http://upload.neopets.com/beauty/images/winners/silkmon-2012-05-11.jpg"
    while not indstop:
        if 1 != 0:
#             pendtop = imgdb.Select_Recs(vselcols = SELIMGPENDSEL, vwhrcols = SELIMGPENDWHR,
#                                     vordcols = SELIMGPENDORD, ftc = btc)
#             imgdb.Kill_Cur(pendtop[1],False) # Locks db for update.
# Compare req-selena.
#             tsturl = r"https://archive.help-qa.com/history/few-replies//8"
#             (verr,tmpbrw) = Req_Page(tsturl)
#             htmlbod1 = tmpbrw.text
#             uti.Write_UTF(BADHTML.format(1,1),htmlbod1,True)
#             (verr,tmpbrw) = Selen_Page(tsturl,tslp=180)
#             galena = tmpbrw
#             htmlbod2 = tmpbrw.page_source
#             uti.Write_UTF(BADHTML.format(1,2),htmlbod2,True)
#             galena.quit()
# Find test.
#             tstt = Find_Loop_Repl("""One of you, "ladies'""","s{punc}",1, False, 0)
#             tstt = Find_Loop_Repl(Load_Dummy(1),"""class={punc}ipsDataItem """,1, False, 133541)
#             print(tstt)
# Dummy.
#            verr = Build_Crawl(dbrefs,btc,ddl)
#            verr = Build_List(dbrefs,lwin,ddl)
#            verr = Build_List(dbrefs,lfail,ddl)
#            verr = Grab_Webfile(tstdl)
#            verr = Seize_Links(dbrefs)
#             verr = Mine_Yeda(dbrefs,1)
#             verr = Mine_List(dbrefs)
#             verr = Build_Crawl(dbrefs,922,10)
#             verr = Close_Dupe_Topics(dbrefs)
#             verr = Read_All_Topics(dbrefs,pendtop)
# Selects.
#             verr = Review_Dbs(dbrefs,[1,2,3])
            print(verr)
            indstop = True # TEST
        else:
            indstop = True # All done.
            
    
    tdiff = uti.Ed_Timer("Main")
    print("\nFin.")
    logger.debug("End Main_FT {}".format(tdiff)) # Remember that timestamp is automatic in logger.
    
    return verr

def Main():
    """Activates function calls.
    
    Main."""
    Deb_Prtlog("Start Main_FT",logging.DEBUG)
    uti.St_Timer("Main")
    
    verr = 0
#     dbrefs = Init_Parms()
    if FMAIN in (1,2,3,9):
#         verr = Mine_Yeda(dbrefs,FMAIN)
        verr = 0
    elif FMAIN == 8:
#         verr = Mine_List(dbrefs)
        verr = 0
    elif FMAIN == 11:
#         verr = Review_Dbs(dbrefs,QUERYLIST)
        verr = 0
    
    tdiff = uti.Ed_Timer("Main")
    print("\nFin.")
    logger.debug("End Main_FT {}".format(tdiff)) # Remember that timestamp is automatic in logger.
    if verr == 0:
        uti.Msgbox(LOGMSG["mainok"],"Good morning chrono")
    else:
        uti.Msgbox(LOGMSG["mainer"],"Wake up")
    
    return verr
    
if __name__ == "__main__":
    if DEVMODE:
        Main_Test()
    else:
        Main()
    
