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
#                        format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
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
("%Y/%m/%d %H:%M:%S",0), # FIRST = PRINT FORMAT. Rev gregorian - better for comparison.
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
MINTS = 1000000000 # Avoiding reg numbers, for lack of more intelligent type detection.
MAXTS = 4102444800 # 01/01/2100.
MINDT = pd.datetime(1970,1,2,2,0) # Below this causes oserr on ts.
DATETYPE = MINDT.__class__
TIMETYPE = NOTIME.__class__
# Keys which are created with a default value when nonexistent.
# Unlike utils, accepts a variable key as parm (incorporates dview2).
FDEFS = {
"Pddiric":{"group":("group",1),"weight":("weight",1)}
}
BETAPARM = (2,2)
CHKTYP = 3 # Number of elems to assert dtype.

# BM! Defs

class BmxFrame(pd.DataFrame):
    """Dataframe with some tricky methods.
    
    Spam."""
    def __init__(self,*lparms,**parms):
        """Init with additional empty row option.
        
        Gets both list and dict to inherit positional as is."""
        if "rcnt" in parms:
            parms["index"] = np.arange(parms["rcnt"])
            parms.pop("rcnt")
        super().__init__(*lparms,**parms)
        
    def Get_Key(self,k,kdef = None):
        """Get key from frame, or use as series.
        
        Method of obtaining it is a tad shoddy.
        Alt: In def mode, k is a dict, and fills cols
        with the default value per key."""
        if kdef:
            ddef = k
            for (k,(col,vd)) in FDEFS[kdef].items():
                indfil = False
                if k not in ddef:
                    indfil = True
                    ddef[k] = col
                else:
                    col = ddef[k] 
                    if col not in self.columns:
                        indfil = True
                if indfil:
                    self[col] = vd
            return ddef
        else:
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
        return BmxFrame(self.merge(df2,on = "fakey",**parms))
    
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
    
    def Rand_DirichletG(self,**parms):
        """Selects random length segments to fill a line (sum = 1).
        
        Valid keys are group for applying each rand per segment,
        and weight which gives higher priority to certain parts.
        Alt: There's np.random.dirichlet(weight), but no group equiv.
        Also uses beta distro due to its [0,1] interval output;
        Alpha >> beta => values lean to the right and vice versa,
        alpha ~ beta and large => central lean, small => either edge,
        a = b = 1 seems to be uniform.
        Not sure how it should be done formally, but this ought to work,
        can't really use as alpha directly in groups."""
        rund = self.Get_Key(parms,"Pddiric")
        self["seed"] = np.random.beta(*BETAPARM,len(self))
        self["seed"] = self["seed"] * self["weight"]
        ssum = self.groupby(rund["group"])["seed"].sum()
        # Though setting both frames to grp idx would suffice for operations,
        # cannot obtain the original index from it.
        ssum.rename("ssum",inplace = True)
        df2 = df.merge(ssum,left_on = rund["group"],right_index = True)
        vret = df2["seed"] / df2["ssum"]
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
        ncol.loc[msk] = pd.to_timedelta(ocol.loc[msk] + ":00",unit = unit,
                                        errors = "coerce")
        return ncol
    
    def Fill_Rept(self,vpart):
        """Creates a column with repeating values from a partial list. 
        
        Simple trick - sort by mod, ffill and sort back.
        Since none is typeless, col creation attempts to coerce per the given value,
        and defaults to float (prolly)."""
        if uti.isstr(vpart): # Existing col, presumably.
            bscol = self[vpart]
        else:
            # Not sure why, but pd.NaT yields an error:
            # TypeError: float() argument must be a string or a number, not 'NaTType'
            try:
                bscol = BmxFrame(data = None,index = self.index,
                                 dtype = pd.Series(vpart[:CHKTYP]).dtype,
                                 columns = ["filler"])
            except ValueError: # Int lacks null.
                bscol = BmxFrame(data = None,index = self.index,
                                 columns = ["filler"])
            bscol["filler"][:min(len(vpart),len(self))] = vpart
        vcnt = bscol["filler"].count() # Count ignores nulls.
        if vcnt >= len(self) or vcnt == 0:
            vret = bscol["filler"] # Was filled to the brim earlier, or left empty.
#         elif vcnt == 0:
#             vret = bscol
#             return vret
        else: 
            bscol["tmp"] = np.arange(len(self)) % vcnt
            vret = bscol.sort_values("tmp")["filler"].ffill().sort_index()
            
        return vret
    
    def Expandong(self,kin):
        """Creates a frame based on a counter key.
        
        Each original index shall appear in as many rows as the count,
        with a an additional count col (can be appended to idx later if desired).
        Trick: Create the total size, find and fill the edges using cumsum,
        and fill in between.
        Bug: Left merge with left idx + right on yields a messed up index.
        A workaround of left_on = vret.index doesn't yield proper results either.
        Hence, another column needs to be added.
        (Orig index, since it isn't preserved.)
        Bug: An automatic (arange) index is transformed to """
        vcol = self.Get_Key(kin)
        vcol = vcol[vcol != 0] # Empty entries disrupt the merge.
        vcnt = int(sum(vcol))
        vret = BmxFrame(data = 1,index = np.arange(vcnt) + 1,columns = ["sylladex"])
        vret["pdx"] = vret.index  
        vcolsum = vcol.cumsum().astype(int)
        idxnm = vcolsum.index.name # Should be same as main.
        if idxnm is None:
            idxnm = "index" # A default.
        vret = vret.merge(vcolsum.reset_index(),how = "left",
                          left_on = "pdx",right_on = vcolsum.name)
        vret[idxnm].bfill(inplace = True)
        # Int cannot contain nans, and therefore has to be reset after processing.
        vret[idxnm] = vret[idxnm].astype(vcol.index.dtype)
        vret.set_index(idxnm,inplace = True)
        vret["sylladex"] = vret.groupby(vret.index)["sylladex"].cumsum()
        return BmxFrame(vret["sylladex"])
    
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
    df["group"] = (df.index // 3)
    df["diric"] = df.Rand_DirichletG(group = "group")
    print("Dirichlet:\n",df[["diric"]],"=",sum(df["diric"]))
    df["partfill"] = df.Fill_Rept([1,2,3])
    print("Repeating fill:\n",df[["partfill"]])
    df2 = BmxFrame(index = ["elephant","tiger","bear"])
    #df2 = BmxFrame(index = [0.1,0.2,0.3])
    df2["addcnt"] = np.arange(len(df2)) + 1
    df2a = df2.Expandong("addcnt")
    print("Added rows:\n",df2a) 
    print("\nFin")
else:
    pass
    
# FIN