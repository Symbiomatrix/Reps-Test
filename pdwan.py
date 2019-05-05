# -*- coding: cp1255 -*-
#!! -*- coding: utf-8 -*-
"""
Created on 26/03/19

@author: Symbiomatrix

@purpose: Pandas shenanigans.

Todo:
More stuff.

Future:

Notes:

Bugs: See those marked below and workarounds.

Version log:
03/05/19 V0.7 Added date ceil / floor, sparse time conversion.
30/04/19 V0.6 Added group (max row) selection, date + time type format, partial fillna.
20/04/19 V0.5 Added cooldown.
08/04/19 V0.4 Added period overlap, simple save frame.
05/04/19 V0.3 Added fill by repetition, expansion.
03/04/19 V0.2 Added various randomisation methods.
28/03/19 V0.1 Added basic time utils, cross join, conversion.
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
import re
import utils as uti

logger.debug("Start pdwan module")

# BM! Consts
Deb_Prtlog = lambda x,y = logging.ERROR:uti.Deb_Prtlog(x,y,logger)
NOTIME = pd.Timedelta(0)
SEC = pd.Timedelta(1,"s")
MIN = pd.Timedelta(1,"m")
MIN2 = 60
HOUR = pd.Timedelta(1,"h")
HOUR2 = 60 * MIN2
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
PDDTDTYP = "datetime"
PDTMDTYP = "timedelta"
EPS = sys.float_info.epsilon
# Has keys which are created with a default value when nonexistent.
# Unlike utils, accepts a variable key as parm (incorporates dview2).
uti.FDEFS.update({
"Pddiric":{"group":("group",1),"weight":("weight",1)},
"Pdolap":{"id":"id","tstart":"tstart","tend":"tend","rdur":"rdur",
          "tseq":None},
"Pdmaxr":{"group":None,"maxcol":"maxcol","func":"idxmax",
          "flocs":[],"fparms":dict(),"dedix":False},
"Pdtrigcd":{"cdur":MIN,"group":None,"tstamp":"tstart", # Also kin.
            "pgroup":None,"pcd":NOTIME,"ptstamp":MINDT},
})
uti.LOGMSG.update({
"pdsvper": "Could not save frame - file is open: {}",
})
BETAPARM = (2,2)
CHKTYP = 3 # Number of elems to assert dtype.
FEXT = ".csv"
GRPIDX = {"head","tail"} # Group functions which return the original index as index.
# Compare to eg idxmax, which leaves the group intact and value is the index.
DCOOL = dict()

# BM! Defs

def Appcd(drow): #,**parms)
    """Applies cooldown to sets of rows.
    
    Retains the previous col vals and cooldown,
    using a global dict.
    Kwargs by spec are shallow copied (mostly thread safe),
    and thus improper for use in its place -
    reliance on inner mutable object is shoddy.
    Amongst them cd length, current cd, previous + name of groups and timestamp.
    Alt: Group and time cols can be shifted ahead rather than stored,
    but that's prolly space inefficient and pointless since cd isn't vectorised."""
    global DCOOL
    parms = DCOOL
    vret = drow[parms["kin"]]
    if vret:
        if (parms["pgroup"] == drow[parms["group"]]).all(): # Completely same group.
            ctime = drow[parms["tstamp"]]
            parms["pcd"] = max(NOTIME,parms["pcd"] - (ctime - parms["ptstamp"]))
        else: # Different group, cd irrelevant.
            parms["pcd"] = NOTIME
        if parms["pcd"] > NOTIME: # Still in cd, switch.
            #drow[parms["kin"]] = False
            vret = False
        else:
            parms["pcd"] = parms["cdur"]
        parms["pgroup"] = drow[parms["group"]]
        parms["ptstamp"] = drow[parms["tstamp"]]

    print("Latest iter:",parms)
    return vret

def App_Strftime(dcol,repnull = "",*lparms,**parms):
    """Used to apply strftime to multiple series in frame.
    
    Alt: Loop over the rows and use use dt. Not sure of efficiency.
    The copy just suppresses an annoying warning."""
    vret = dcol.dt.strftime(*lparms,**parms).copy()
    if repnull is not None:
        vret.loc[dcol.isna()] = repnull
    return vret

def Delim_Format(matchobj,schr = "%"):
    """Surrounds an item with format compatible brackets, with escaping.
    
    The special char should be caught by regex as well.
    Handling this without a function is complex - lookahead, lookbehind,
    cases like %%A and %%%A and %%%.
    Since re doesn't forward parms, use a lambda to set schr."""
    if matchobj.group(1) == schr: # Special char escaped.
        return matchobj.group(1)
    else:
        # To clarify, the external 2 pairs are escaped.
        return "{{{obj}}}".format(obj = matchobj.group(1))

def App_Strfdelta(dcol,date_format,repnull = ""):
    """Used to apply a format to timedeltas in frame.
    
    That's not an actual thing, but with template,
    it's possible to access some attributes via a letter.
    Available components: days, hours, minutes, seconds,
    milliseconds, microseconds, nanoseconds.
    Dunno how those work, and milliseconds is absent as an attr.
    There's a string.template thing for this sort formatting using %,
    but it's a bit of a hassle to set up.
    Creep: Read format directly, put precise remainder on lowest denomination -
    for example, weeks + days and partial day."""
    d = BmxFrame({"D":dcol.dt.days})
    d["H"],rem = divmod(dcol.dt.seconds,HOUR2)
    d["M"],d["S"] = divmod(rem,MIN2)
    #d["L"] = drow.dt.milliseconds # Does not exist?
    d["C"] = dcol.dt.microseconds
    d["N"] = dcol.dt.nanoseconds
    d["s"] = dcol.dt.total_seconds()
    stdfmt = re.sub("%(.)",lambda x: Delim_Format(x,schr = "%"),date_format)
    # Displaying either zeroes or empty seems fine by me.
    vret = d.Col_Format(stdfmt,True,"0")
    if repnull is not None:
        vret.loc[d["s"].isna()] = repnull
    return vret

def App_Format(drow,fmt,indcast = False,nullstr = None):
    """Given a row of mixed types: downcasts ints, renames nulls and formats as str.
    
    There are 2 stages to downcast: first, filter out nulls and nonnumbers;
    the former doesn't exist in int (error) and latter cannot be cast (error).
    Then, check if the number if within floating range of the whole.
    Cannot mix types in np, so result is returned as dict.
    Note that numeric cols sometimes return null rather than true,
    unlike strings containing numbers.
    Unfortunately, strings don't have a float check function.
    Elemwise advice is to try-catch cast to float, but astype only has ignore,
    which leaves the value (and type) as str.
    Opted to simulate a part of float cast (sans scientific notation,
    which permits num-e-int)."""
    dictrow = drow.to_dict()
    if indcast:
        chknum = (drow.astype(str).str.replace(".","",n = 1)
                  .str.lstrip("+-").str.isnumeric())
        #chknum = drow.astype(str).str.isnumeric()
        flt = drow[(chknum.isna() | chknum) &
                    ~drow.isna()]
        flt2 = flt[abs(flt.astype(float) - flt.astype(int)) <= EPS]
        flt2 = flt2.astype(int)
        dictflt = flt2.to_dict()
        dictrow.update(dictflt)
    if nullstr is not None:
        flt = drow[drow.isna()]
        flt[:] = nullstr
        dictflt = flt.to_dict()
        dictrow.update(dictflt)
    return fmt.format(**dictrow)

class BmxFrame(pd.DataFrame):
    """Dataframe with some tricky methods.
    
    Overrides some default method output for convenience, then come tricks.
    (This may be possible to do dynamically via list + setattr in a loop,
    but I'm not sure that would fit the compilation.)"""
    def __init__(self,*lparms,**parms):
        """Init with additional empty row option.
        
        Gets both list and dict to inherit positional as is.
        Creep: If df, should prolly not call parent which might implicitly copy.
        But not sure how to create the link without a ref attribute."""
        if "rcnt" in parms:
            parms["index"] = np.arange(parms["rcnt"])
            parms.pop("rcnt")
        super().__init__(*lparms,**parms)
        
    def reset_index(self,*lparms,**parms):
        "Override."
        return BmxFrame(super().reset_index(*lparms,**parms))
    
    def set_index(self,*lparms,**parms):
        "Override."
        return BmxFrame(super().set_index(*lparms,**parms))
    
    def merge(self,*lparms,**parms):
        "Override."
        return BmxFrame(super().merge(*lparms,**parms))
        
    def copy(self,*lparms,**parms):
        "Override."
        return BmxFrame(super().copy(*lparms,**parms))
    
    def fillna(self,value,*lparms,**parms):
        """Override plus support for value = list.
        
        In order to fill part of the frame with differing vals,
        parent expects value = dict."""
        # Worse alts (prolly create a copy):
        # df[cols] = df.fillna(cols)
        # df.fillna({c:0 for c in cols},inplace = True)
        if uti.islstup(value): # Format: First is fill val, rest are cols.
            value = {c:value[0] for c in value[1:]}
        return BmxFrame(super().fillna(value,*lparms,**parms))
        
    def Get_Key(self,k,kdef = None):
        """Get key from frame, or use as series.
        
        Method of obtaining it is a tad shoddy.
        Alt: In def mode, k is a dict, and fills cols
        with the default value per key."""
        if kdef:
            ddef = k
            for (k,(col,vd)) in uti.FDEFS[kdef].items():
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
        
    def Get_Group(self,k,defall = False):
        """Returns list of index / multiindex names or group cols.
        
        Small transference func.
        Also resets index for access if necessary."""
        if k is None:
            if isinstance(self.index,pd.MultiIndex):
                idxnm = self.index.names
            else:
                idxnm = self.index.name
            df = self.reset_index()
            if idxnm is None:
                idxnm = "index" # A default.
                if defall: # Treats all rows as either single group, or separate.
                    df[idxnm] = 1
        else:
            df = self
            idxnm = k
        if not uti.islstup(idxnm):
            idxnm = [idxnm]
        return (idxnm,df)
        
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
        vret = pd.Series(aseed * vstd,index = self.index)
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
        df2 = self.merge(ssum,left_on = rund["group"],right_index = True)
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
            if indts < 2:
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
        Handles multiindex idiosyncrasies: inplace doesn't work and 
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
        if isinstance(vcolsum.index,pd.MultiIndex):
            idxnm = vcolsum.index.names # Picky.
        else:
            idxnm = vcolsum.index.name # Should be same as main.
        if idxnm is None:
            idxnm = "index" # A default.
        vret = vret.merge(vcolsum.reset_index(),how = "left",
                          left_on = "pdx",right_on = vcolsum.name)
        vret[idxnm] = vret[idxnm].bfill()
        # Int cannot contain nans, and therefore has to be reset after processing.
        if uti.islstup(idxnm):
            for i,idx in enumerate(idxnm):
                # BUG: For some reason, next line lags the debug a step.
                orgtyp = vcol.index.get_level_values(i).dtype
                curtyp = vret[idx].dtype
                if orgtyp != curtyp:
                    vret[idx] = vret[idx].astype(orgtyp)
        else:
            vret[idxnm] = vret[idxnm].astype(vcol.index.dtype)
        vret.set_index(idxnm,inplace = True)
        vret["sylladex"] = vret.groupby(vret.index)["sylladex"].cumsum()
        return BmxFrame(vret["sylladex"])
    
    def Period_Overlap(self,**parms):
        """Merges period rows whose times overlap.
        
        Valid parms are id (group, never merged), tstart and tend, rdur -
        all are col names.
        Opt: tseq = Will merge nigh consecutive recs, within certain timespan.
        Expects the frame to be sorted by id + start.
        Returns the full data from the last row in each overlap, arbitrarily.
        Explanation: for each row checks if start is within range of greatest end,
        up to prev row (notwithstanding id); if so, then merge it.
        The group boundaries are defined, earliest start propagated to each,
        and the last of each is filtered.
        Given list of ids, they are treated as a composite key."""
        rund = uti.Default_Dict("Pdolap",parms)
        if not uti.islstup(rund["id"]):
            rund["id"] = [rund["id"]]
        self["firstrec"] = np.any([self[k] != self[k].shift(1)
                                   for k in rund["id"]],axis = 0)
        self["lastrec"] = np.any([self[k] != self[k].shift(-1)
                                  for k in rund["id"]],axis = 0)
        # List seems to suffice.
#         else:
#             # Single col has dim 1 which np flattens,
#             # so must either convert to frame or separate.
#             self["firstrec"] = self[rund["id"]] != self[rund["id"]].shift(1)
#             self["lastrec"] = self[rund["id"]] != self[rund["id"]].shift(-1)
        self["group"] = self["firstrec"].astype(int).cumsum()
        self["tmaxend"] = self.groupby("group")[rund["tend"]].cummax()
        if rund["tseq"] is None: # Must be nonzero overlap.
            self["indmrg"] = (self[rund["tstart"]].shift(-1) >
                              self["tmaxend"])
        else:
            self["indmrg"] = (self[rund["tstart"]].shift(-1) >
                              self["tmaxend"] + self.To_Time(rund["tseq"],"s"))
        self.loc[self["lastrec"],"indmrg"] = True
        self["indmrg2"] = self["indmrg"].shift(1) # First rec of each group.
        self["indmrg2"].fillna(True,inplace = True)
        self["group"] = self["indmrg2"].cumsum()
        self["tminstart"] = self.groupby("group")[rund["tstart"]].cummin() # Alt: First.
        vret = self[self["indmrg"]].copy()
        vret[rund["tstart"]] = vret["tminstart"]
        vret[rund["tend"]] = vret["tmaxend"]
        vret[rund["rdur"]] = vret[rund["tend"]] - vret[rund["tstart"]]
        return BmxFrame(vret)
    
    def Save_Frame(self,fdir):
        """Save frame to file.
        
        Creep: Fix date, time, float formatting."""
        if not fdir.endswith(FEXT):
            fdir = fdir + FEXT
        try:
            self.to_csv(fdir)
        except PermissionError:
            Deb_Prtlog(uti.LOGMSG["pdsvper"].format(fdir),logging.WARN)
        return 0
    
    def Translate_Values(self,kin,dtrans):
        """Translate values based on dict.
        
        Key = original value, value = translated. Nothing fancy."""
        vcol = self.Get_Key(kin)
        return vcol.map(dtrans)
    
    def Group_Rows(self,**parms):
        """Creates a frame with the rows whose value is greatest (and others).
        
        Parms: group = col per each max applied,
        maxcol = col which is supposed to be maxed,
        func + *flocs + **fparms = the comparison method.
        (must return indices, such as idxmax, idxmin, head, tail.)
        First, last = head or tail (1), these don't return idx at all.  
        Group defaults to index, but if there isn't one then picks global max.
        Mind, df must *not* be keyed in order for idxmax to work
        (mayhap iloc would suffice), other than def.
        It's just used as a col and the group later
        assimilates it to the unique frame (keyed).
        Just for fun, these are equivalent calls:
        func = "tail",flocs = 1 ; func = lambda x: x.tail(1),dedix = True."""
        rund = uti.Default_Dict("Pdmaxr",parms)
        if not uti.islstup(rund["flocs"]):
            rund["flocs"] = [rund["flocs"]]
        # Single parm can be converted, but non dict would require keys.
        (idxnm,df) = self.Get_Group(rund["group"],defall = True)
        # Drop dupes is the frame equiv of series.unique.
        # Though I don't actually need it, unless appending cols.
        #vret = BmxFrame(df[idxnm].drop_duplicates(),columns = idxnm)
        #vret.set_index(idxnm,inplace = True)
        tidx = df.groupby(idxnm)[rund["maxcol"]]
        if uti.isstr(rund["func"]): # Interpreted as method name. 
            func = getattr(tidx,rund["func"])
            tidx = func(*rund["flocs"],**rund["fparms"])
        else: # Otherwise, a function which should handle the series.
            func = rund["func"]
            tidx = func(tidx,*rund["flocs"],**rund["fparms"])
        if (rund["func"] in GRPIDX
            or rund["dedix"]): # May force idx grab in custom funcs.
            tidx = tidx.index
        tgrp = df.loc[tidx]
        tgrp.set_index(idxnm,inplace = True)
        vret = tgrp
        #vret[newcol] = tgrp # Append method.
        
        return vret
    
    def Trigger_Cooldown(self,kin,**parms):
        """Applies cooldown to a boolean col.
        
        In col kin, will leave only the first indication on
        for every series of length cdur (important);
        based on time from col tstamp (p) and treating group (p) as separate.
        Def group is the index. Should be sorted by group-tstamp.
        This is a SLOW function, resorting to apply,
        yet potentially hastened vastly by filtering positive rows only.
        Since order is expected, apply might not be the best choice over loop,
        seemed pretty safe in my experiments however."""
        global DCOOL
        rund = uti.Default_Dict("Pdtrigcd",parms)
        rund["kin"] = kin
        (idxnm,df) = self.Get_Group(rund["group"],defall = True)
        rund["group"] = idxnm
        df = BmxFrame(df.loc[df[kin]]) # Cont: Does it create a copy?
        #func = lambda x: Appcd(x,**rund) # Kwargs creates a fresh copy regardless.
        DCOOL = dict()
        DCOOL.update(rund)
        vret = self[kin].copy()
        # In frame, a set clause works, but in series that creates a new row.
        cdblock = df.apply(Appcd,axis = 1)
        if len(cdblock) > 0: # Empty frame => no triggers.
            #vret[kin] = cdblock # Frame only.
            vret.update(cdblock) # Series only.
        return vret
    
    def Type_Format(self,fmtdate = None,fmttime = None,repnull = ""):
        """Reformats all cols of certain type inplace.
        
        This can be used for example in to_csv or to_json:
        there is a date_format and date_unit respectively,
        but both accept only specific values, like iso / epoch,
        and nulls lack variation (translated to text None).
        Creep: Float format."""
        # CONT: null handle.
        if fmtdate is not None:
            subcols = self.select_dtypes(PDDTDTYP)
            # Alt: self[subcols.columns] seems to work just fine.
            self.loc[:,self.columns.isin(subcols.columns)] = (
                subcols.apply(App_Strftime,date_format = fmtdate,repnull = repnull))
        
        if fmttime is not None:
            subcols = self.select_dtypes(PDTMDTYP)
            self.loc[:,self.columns.isin(subcols.columns)] = (
                subcols.apply(App_Strfdelta,date_format = fmttime,repnull = repnull))
    
    def Col_Format(self,fmt,indcast = False,nullstr = None):
        """Creates a column of a string format.
        
        The frame's columns may be referenced by name, ie {col}.
        Trick is to apply by row, and unpack its cols to dict.
        Caveat: During apply, integers are instantly degraded to floats.
        In a colwise setting, this could be fixed by checking type
        (col.dtype == float) and then value (col == col.astype(int).all()),
        but in a row series the type is singular for all vals,
        some of which may actually be float.
        As such, created a special format function which also downcasts conditionally,
        upon request - additional conversion to dict may delay the process a bit.
        Additionally, formatter converts nan / nat to strings."""
        return self.apply(App_Format,1,fmt = fmt,indcast = indcast,nullstr = nullstr)
        #return self.apply(lambda x:fmt.format(**x),1)
    
    def Ceil_Date(self,kin,scdt):
        """Upper bound to a date series by scalar.
        
        Normal min doesn't work since the indices don't match.
        Np min / max complains anent 'int-timestamp', yet won't permit converting either
        (in a prior version, int cast seemed to work).
        Currently, converting to a series works."""
        vdt = self.Get_Key(kin)
        serdt = pd.Series(pd.to_datetime(scdt))
        return np.minimum(vdt,serdt)
    
    def Floor_Date(self,kin,scdt):
        """Lower bound to a date series by scalar.
        
        Equiv to ceil."""
        vdt = self.Get_Key(kin)
        serdt = pd.Series(pd.to_datetime(scdt))
        return np.maximum(vdt,serdt)
    
    def Date2Time_Sparse(self,kin,fmt = None,klean = False):
        """Converts an array / col of dupe dates to time or lean frame.
        
        Col must be sorted asc to fill correctly.
        In lean mode, each dupe is reduced to first, min, max, last;
        Which preserves the graph for display, and far less demanding.
        Send a key name for the value to be maxed in this case.
        Originally intended for array of datetime64, that should work.
        If format is not passed, will convert to actual time,
        which aligns nicely in pyplot (despite its drawbacks - object?).
        Creep: Lean form might be inefficient.
        Stack to a predefined array with 4 cols for the types, along axis 1, 
        and finally reshape to (-1,1);
        then for time axis, pick every {dense} value (::dense), reshape to (-1,1),
        tile (4,1) and reshape -1 - this will merge sequentially per row for both.
        Fun format, named "caledfwelch" - min max only,
        merge along axis 0 (or transpose)."""
        vdt = self.Get_Key(kin) # Np -> pd is fast.
        if klean:
            # Kin must be referrable by name in this case.
            vgrp = self.Group_Rows(group = vdt.name,maxcol = klean,
                                   func = "head",flocs = 1) # Alt: first of group.
            vgrp["skey"] = 1
            vret = vgrp
            vgrp = self.Group_Rows(group = vdt.name,maxcol = klean,
                                   func = "idxmin")
            vgrp["skey"] = 2
            vret = vret.append(vgrp)
            vgrp = self.Group_Rows(group = vdt.name,maxcol = klean,
                                   func = "idxmax")
            vgrp["skey"] = 3
            vret = vret.append(vgrp)
            vgrp = self.Group_Rows(group = vdt.name,maxcol = klean,
                                   func = "tail",flocs = 1)
            vgrp["skey"] = 4
            vret = vret.append(vgrp)
            # No index + col sort, so other options are np.lexsort,
            # or 2 part + mergesort on the col which is stable.
            vret.reset_index(inplace = True)
            vret.sort_values([vdt.name,"skey"],inplace = True)
            vret.set_index(vdt.name,inplace = True)
        else:
            self["tmp"] = pd.NaT
            # Dt functions are slow by slow, the dupe reduction helps as filter is fast.
            if fmt is not None:
                self["tmp"] = App_Strftime(self.loc[vdt != vdt.shift(1),vdt.name],
                                           date_format = fmt)
            else:
                self["tmp"] = self.loc[vdt != vdt.shift(1),vdt.name].dt.time
            self["tmp"].ffill(inplace = True)
            vret = self["tmp"]
        return vret
    
    def Misc_Repo(self):
        """Some other ad hoc function ideas.
        
        """
#         -df.to_json(orient = "type") options:
#         index = key is the main, cols inside named.
#         columns = opposite of index, cols -> key.
#         records = index is dropped (made into list), cols named.
#         values = L2 only, no keys or col names whatsoever.
#         table = "schema" header item defining col structure,
#         but repeated in data (= records).
#         DO NOT just apply json to a list of dfs!
#         The string output must be matched manually.
        # -Convert pd.NaT to None so that it can be json serialised.
        # Necessary when multiple frames have to be merged; and also supports indent.
        #json.dumps(df.where(df.notnull(),None).to_dict(orient="index"),indent = 4)
        #df.to_json(orient = "index") # Alt if only one frame.
        # -Nice shorthand for set, but generates warnings of implicit slice copy update.
        # Note that update uses left join by index only (kinda crude method).
        #df.update(func(df.filter)) == df.loc[:,df.filter.columns] = df[df.filter]
        # -Old bug: In past versions (prolly), the timedelta didn't cancel out in df,
        # as it did in series. Now both return floats.
        #df[["tdur","tdur"]] / SEC
        # Isoformat still not ported to .dt.
        # -Old bug: Could not divide one timedelta by another - yielded error,
        # rather than float ratio of duration. It works as expected now.
        # Workaround was to divide both by 1 sec to obtain a float.
        #df["tdur"] / (df["tdur"] - 30 * MIN)
        # -Rearrange columns; warning, will create null cols if nonexistent.
        #df.reindex(columns = ["col1","col2"])
        # -Derive a timestamp from date (for its hours, mins and secs).
        # "Time" type is a dead end - for example lacks any sort arithmetic.
        #(serdt - pd.datetime.utcfromtimestamp(0)).dt.total_seconds()
        # Derive time of day (hacky, my new type format is more elegant if looped):
        #pd.Timedelta(pd.to_datetime(serdt).dt.strftime("%H:%M:%S"))
        # -Bug: Timedelta type is quite sensitive. astype(np.timedelta64)
        # or astype("timedelta64") will result in a float of seconds (ie e11).
        # To get the expected timedelta, send astype("timedelta64[ns]")
        # -Quick hack for date -> time along the same day:
        #daydt = pd.to_datetime(to_dt(t[0]).strftime("%Y-%m-%d")) # Earliest day ts.
        #df["puret"] = df["rawt"] - daydt 
        # -?Bug: "Time" type is limited to checks, therefore arithmetic
        # timedelta lacks any summary;
        # Best is the altchecktact method whilst I wrote bug.
        # [Dunno what this means exactly, but presumably issue with
        # the inaccessibility of methods for "dt.time" col, eg sum.]
        # -Moving average.
        # N is window size (number of periods averaged,
        # M (= N def) is number (-1) of nans until giving an output;
        # if m < n, will divide value by the current count. 
        # Fails (notimp) if applied to date, tdel etc.
        #df.rolling(window = {n},min_periods = {m}).mean()
        # -Top percentile (technically quantile).
        # prc = 0.5 => median. Filters to numeric cols only,
        # but can be forced to timedeltas.
        #df.quantile(q = {prc})
    
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
    df3 = BmxFrame(rcnt = 10)
    df3["id"] = [10] * 5 + [20] * 5
    df3["tstart"] = (df3.To_Date(["2010-01-01 15:55:00"] * 8 +
                                 ["2010-01-01 16:00:00"] + 
                                 ["2010-01-01 16:47:00"]) + df3.index * MIN)
    df3["tend"] = (df3.To_Date(["2010-01-01 16:55:00"] * 8 +
                               ["2010-01-01 16:55:30"] +
                               ["2010-01-01 17:00:00"]))
    df3a = df3.Period_Overlap(id = "id")#,tseq = df3.To_Time(30,"s"))
    print("Overlap merge:\n",df3a[["id","tstart","tend","rdur"]])
#     df3b = df3.set_index(["id","tend"]).Group_Rows(maxcol = "tstart")
    df3b = df3.Group_Rows(group = ["id","tend"],maxcol = "tstart",
                          func = "head",flocs = 2)
    print("Max merge:\n",df3b)
    df3["boolsheep"] = False
    df3.loc[0:6,"boolsheep"] = True
    df3["trig"] = df3.Trigger_Cooldown("boolsheep",cdur = 2 * MIN,
                                       group = None) # Any of ["tstart","id","group"].
    print("TRIGGERED:\n",df3[["id","tstart","boolsheep","trig"]])
    df4 = df3.copy()
    df4["partial"] = np.floor(df4.Rand_Norm() * 5)
    df4.loc[0,"partial"] = pd.NaT
    df4.loc[3,"partial"] = pd.NaT
    # Bug: When value contains nulls, mean raises an exception (unlike sum / cnt).
    # I also mentioned another one when "id lacks some rows", dunno what that was.
    # print("Dataerror:",df4.groupby("id")["partial"].mean())
    df4["tdur"] = df4["tend"] - df4["tstart"]
    # Bug: This also fails, for no good reason. Average time is a legitimate detail.
    # print("Dataerror:",df4.groupby("id")["tdur"].mean())
    df4.fillna([1.5,"trig","id"],inplace = True)
    #df4.fillna({"id":2,"partial":1.5},inplace = True)
    df4.loc[0,"tdur"] = pd.NaT
    df4.loc[3,"tend"] = pd.NaT
    df4.loc[6,"tdur"] = df4.loc[6,"tdur"] - 53914 * SEC
    df4["tstart"] = (df4.Ceil_Date(df4.Floor_Date(
                        "tstart","2010-01-01 16:00"),
                        "2010-01-01 16:30"))
    df4["val"] = df4.Rand_Norm()
    qktm = df4.Date2Time_Sparse("tstart",fmt = "%d %M-%H",klean = False)
    dlean = df4.Date2Time_Sparse("tstart",fmt = "%d %M-%H",klean = "val")
    print("Quick time conv and lean form:\n",qktm,"\n",dlean)
    df4.Type_Format("%y%m%d %H%M%S","%D days, %H hours, %M:%S",None)
    print("Datetime format:\n",df4[["id","tstart","tend","tdur"]])
    print("\nFin")
else:
    pass
    
# FIN