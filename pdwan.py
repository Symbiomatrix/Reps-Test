# -*- coding: cp1255 -*-
#!! -*- coding: utf-8 -*-
"""
Created on 26/03/19

@author: Symbiomatrix

Todo:
- Class for special pd tricks.

Features:

Future:

Notes:

Bugs:

Version log: 
26/03/19 V0.0 New.

"""

# BM! Imports

import sys
import logging
logger = logging.getLogger(__name__) # Separated logger, does not spam main.
if __name__ == '__main__':
#    logging.basicConfig(stream=sys.stderr,
#                    level=logging.ERROR, # Prints to the console.
#                    format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
    logger.setLevel(logging.DEBUG)
#    logging.basicConfig(filename="errlog.log",
#                        level=logging.DEBUG, # Prints to a file. Should be utf. #.warn
#                        format="%(asctime)s %(levelname)s:%(name)s:%(message)s") # Adds timestamp.
SYSENC = "cp1255" # System default encoding for safe logging.
FRMTMLOG = "%y%m%d-%H%M%S" # Time format for logging.
PRTLIM = 1000 # When printing too long a message, will invoke automatic clear screen.
BADLOG = "Utilog-d{}.txt"

import numpy as np
import pandas as pd
import utils as uti

logger.debug("Start pdwan module")

# BM! Consts
NOTIME = pd.Timedelta(0)
SEC = pd.Timedelta(1,"s")
MIN = pd.Timedelta(1,"m")
HOUR = pd.Timedelta(1,"h")
DAY = pd.Timedelta(1,"d")
LITESTR = "\'{}\'" # Extra date conversion.
REGDATES = [ # Regular datetime formats.
("%Y/%m/%d %H:%M:%S",0), # FIRST = PRINT FORMAT. Reverse gregorian - better for comparison.
("%Y/%m/%d",0), # Rev greg date.
("%d/%m/%Y %H:%M:%S",0), # Gregorian.
("%d/%m/%Y",0), # Gregorian date.
("%Y-%m-%d %H:%M:%S.%f",0), # Result of str(dtm).
("%Y-%m-%d %H:%M:%S",0), # (Roughly) Iso 8601.
("%Y-%m-%d",0), # Iso 8601 date.
("%Y.%m.%d %H:%M:%S",0), # Another common greg format.
("%Y.%m.%d",0),
("%d.%m.%Y %H:%M:%S",0),
("%d.%m.%Y",0),
("%d/%m/%y",1)
]
DSPDATE = REGDATES[0][0] # %f for ms. Can rstrip time away.
MINTS = 86400 # Any less fails with oserr 22.
MINTS = 1000000000 # Avoiding regular numbers, for lack of more intelligent type detection.
MAXTS = 4102444800 # 01/01/2100.
MINDT = pd.datetime(1970,1,2,2,0) # Below this causes oserr on ts.
DATETYPE = MINDT.__class__
TIMETYPE = NOTIME.__class__

# BM! Defs

class BmxFrame(pd.DataFrame):
    """Dataframe with some tricky methods.
    
    Spam."""
    def __init__(self,**parms):
        """Init with additional empty row option."""
        if "rcnt" in parms:
            parms["index"] = np.arange(parms["rcnt"])
            parms.pop("rcnt")
        super().__init__(**parms)
        
    def Get_Key(self,k):
        """Get key from frame, or use as series.
        
        Method of obtaining it is a tad shoddy."""
        if k is None:
            return k
        try: # As a key.
            return self[k]
        except (KeyError,ValueError): # As series object.
            return pd.Series(k,index = self.index)
        
    def Hash_Ids(self,kout = "hshid",indrnd = True):
        """Creates hashed ids for the frame, inplace.
        
        Stores the seed used to generate them as well."""
        if indrnd:
            aseed = np.random.rand(len(self))
        else: # Use the index as a unique series for 'guaranteed' randomness.
            aseed = self.index
        self["seed"] = aseed
        self[kout] = self["seed"].apply(uti.Hash_Digits)
        return self[kout]
        
    def Cross_Join(self,df2,**parms):
        """Combine each two rows of the frames.
        
        Aka cartesian product for all you nerds."""
        self["fakey"] = 0 # Same key for all rows.
        df2["fakey"] = 0
        return self.merge(df2,on = "fakey",**parms)
    
    def Rand_Norm(self,vmu = 0,vstd = 1,indint = False):
        """Gaussian distribution with mu and std.
        
        Optionally generates 'natural' numbers only,
        which uses symmetric rand, and mu is the min."""
        aseed = np.random.standard_normal(len(self))
        vret = pd.Series(aseed * vstd)
        if indint:
            vret = abs(vret)
        vret = vret + vmu
        if indint:
            vret = np.floor(vret).astype(np.int64)
        
        return vret
    
    def Rand_Time(self,ubnd = DAY):
        """Uniform distro of time up to ubound.
        
        The col may then be added to a date."""
        aseed = np.random.uniform(size = len(self))
        self["seed"] = aseed
        vret = aseed * self.To_Time(ubnd)
        return vret
    
    def To_Date(self,kin,frmt = None):
        """Creates datetime object from possible formats.
        
        Returns nat if no formats fit, to preserve object type.
        Error coercion handles strptime's issues."""
        vdt = self.Get_Key(kin)
        odt = vdt
        #odt = pd.Series(vdt)
        ndt = pd.Series(data = pd.NaT,index = odt.index)
        if vdt is None: # Watch out for later expectations.
            return vdt
        elif isinstance(vdt,DATETYPE):
            return vdt
        elif isinstance(vdt,pd.Series):
            if np.issubdtype(odt.dtype,np.datetime64):
                return vdt
        if frmt is not None:
            return pd.to_datetime(vdt,format = frmt, errors = "coerce")
#             except ValueError:
#                 pass
#             except TypeError: # Strp does not accept aught but str.
#                 pass
        else:
            # First format to eliminate is timestamp.
            # In current version, seems numbers are false dates.
            msk = ndt.isnull()
            indts = 0
            if np.issubdtype(odt.dtype,np.number): # Numbers can be altered freely.
                indts = 2 # No date check.
            else: # Only numerical strings can be compared.
                indts = 1
                try:
                    msk = msk & odt.str.isnumeric()
                except AttributeError: # Not str - date etc.
                    indts = 0
            if indts > 0:
                # Check if numerical rows within expected range, otherwise just a number.
                # Implicit conversion to num works fine.
                msk = msk & ((odt.loc[msk] >= MINTS) &
                             (odt.loc[msk] <= MAXTS))

                # format = secs, aka unix timestamp. Default is ms.
                ndt.loc[msk] = pd.to_datetime(odt,unit = "s",errors = "coerce")
            elif indts < 2:
                for rfrmt in REGDATES:
                    # Of the remaining unknown values, attempt conversion.
                    msk = ndt.isnull()
                    if not msk.any(): # All dates converted.
                        return ndt
                    ndt.loc[msk] = pd.to_datetime(odt,format = rfrmt[0],
                                                  errors = "coerce")
                    msk = ndt.isnull()
                    ndt.loc[msk] = pd.to_datetime(odt,format = LITESTR.format(rfrmt[0]),
                                                  errors = "coerce")
                
        return ndt
    
    def To_Time(self,kin,unit = "h"):
        """Creates timedelta object.
        
        Formatting is only for numbers - unit can be Y, M, D etc.
        Strings are only completed to hours, otherwise automatic match.
        Returns nat if no formats fit, to preserve object type."""
        vcol = self.Get_Key(kin)
        ocol = vcol
        ncol = pd.Series(data = pd.NaT,index = self.index)
        ncol = pd.to_timedelta(ocol,unit = unit,errors = "coerce")
        if vcol is None: # Watch out for later expectations.
            return vcol
        elif isinstance(vcol,TIMETYPE):
            return vcol
        elif isinstance(vcol,pd.Series):
            if np.issubdtype(ocol.dtype,np.timedelta64):
                return vcol
        msk = ncol.isnull()
        ncol.loc[msk] = pd.to_timedelta(ocol.loc[msk] + ":00",unit = unit,errors = "coerce")
        return ncol
    
# Checks whether imported.
if __name__ == '__main__':
    print("hello world")
    df = BmxFrame(rcnt = 10)
    df.Hash_Ids(indrnd = False)
    print("Random ids: ",df["hshid"][:3])
    scrs = df.Cross_Join(df)
    print("Double cross: len =",len(scrs),"cols =",scrs.columns)
    df["rndnum"] = df.Rand_Norm()
    df["rndint"] = df.Rand_Norm(-10,5,indint = True)
    print("Random vals:\n",df[["rndnum","rndint"]][:3])
    df["rndts"] = df.Rand_Norm(MINTS,(MAXTS - MINTS) / 2,indint = True)
    df["getdt"] = df.To_Date("rndts")
    df["gettm"] = df.To_Time("rndts","s")
    df["rndts2"] = df.Rand_Time()
    print("Converted to time:\n",df[["getdt","gettm","rndts2"]])
    print("\nFin")
else:
    pass
    
# FIN