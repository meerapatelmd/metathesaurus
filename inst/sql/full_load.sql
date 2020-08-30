load data local infile '@filePath/MRCOLS.RRF' into table MRCOLS fields terminated by '|' ESCAPED BY '' lines terminated by '\n'
(@col,@des,@ref,@min,@av,@max,@fil,@dty)
SET COL = NULLIF(@col,''),
DES = NULLIF(@des,''),
REF = NULLIF(@ref,''),
MIN = NULLIF(@min,''),
AV = NULLIF(@av,''),
MAX = NULLIF(@max,''),
FIL = NULLIF(@fil,''),
DTY = NULLIF(@dty,'');

load data local infile '@filePath/MRCONSO.RRF' into table MRCONSO fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@cui,@lat,@ts,@lui,@stt,@sui,@ispref,@aui,@saui,@scui,@sdui,@sab,@tty,@code,@str,@srl,@suppress,@cvf)
SET CUI = @cui,
LAT = @lat,
TS = @ts,
LUI = @lui,
STT = @stt,
SUI = @sui,
ISPREF = @ispref,
AUI = @aui,
SAUI = NULLIF(@saui,''),
SCUI = NULLIF(@scui,''),
SDUI = NULLIF(@sdui,''),
SAB = @sab,
TTY = @tty,
CODE = @code,
STR = @str,
SRL = @srl,
SUPPRESS = @suppress,
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRCUI.RRF' into table MRCUI fields terminated by '|' lines terminated by @LINE_TERMINATION@
(@cui1,@ver,@rel,@rela,@mapreason,@cui2,@mapin)
SET CUI1 = @cui1,
VER = @ver,
REL = @rel,
RELA = NULLIF(@rela,''),
MAPREASON = NULLIF(@mapreason,''),
CUI2 = NULLIF(@cui2,''),
MAPIN = NULLIF(@mapin,'');

load data local infile '@filePath/MRCXT.RRF' into table MRCXT fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@cui,@sui,@aui,@sab,@code,@cxn,@cxl,@mrcxtrank,@cxs,@cui2,@aui2,@hcd,@rela,@xc,@cvf)
SET CUI = NULLIF(@cui,''),
SUI = NULLIF(@sui,''),
AUI = NULLIF(@aui,''),
SAB = NULLIF(@sab,''),
CODE = NULLIF(@code,''),
CXN = NULLIF(@cxn,''),
CXL = NULLIF(@cxl,''),
MRCXTRANK = NULLIF(@mrcxtrank,''),
CXS = NULLIF(@cxs,''),
CUI2 = NULLIF(@cui2,''),
AUI2 = NULLIF(@aui2,''),
HCD = NULLIF(@hcd,''),
RELA = NULLIF(@rela,''),
XC = NULLIF(@xc,''),
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRDEF.RRF' into table MRDEF fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@cui,@aui,@atui,@satui,@sab,@def,@suppress,@cvf)
SET CUI = @cui,
AUI = @aui,
ATUI = @atui,
SATUI = NULLIF(@satui,''),
SAB = @sab,
DEF = @def,
SUPPRESS = @suppress,
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRDOC.RRF' into table MRDOC fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@dockey,@value,@type,@expl)
SET DOCKEY = @dockey,
VALUE = NULLIF(@value,''),
TYPE = @type,
EXPL = NULLIF(@expl,'');

load data local infile '@filePath/MRFILES.RRF' into table MRFILES fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@fil,@des,@fmt,@cls,@rws,@bts)
SET FIL = NULLIF(@fil,''),
DES = NULLIF(@des,''),
FMT = NULLIF(@fmt,''),
CLS = NULLIF(@cls,''),
RWS = NULLIF(@rws,''),
BTS = NULLIF(@bts,'');

load data local infile '@filePath/MRHIER.RRF' into table MRHIER fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@cui,@aui,@cxn,@paui,@sab,@rela,@ptr,@hcd,@cvf)
SET CUI = @cui,
AUI = @aui,
CXN = @cxn,
PAUI = NULLIF(@paui,''),
SAB = @sab,
RELA = NULLIF(@rela,''),
PTR = NULLIF(@ptr,''),
HCD = NULLIF(@hcd,''),
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRHIST.RRF' into table MRHIST fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@cui,@sourceui,@sab,@sver,@changetype,@changekey,@changeval,@reason,@cvf)
SET CUI = NULLIF(@cui,''),
SOURCEUI = NULLIF(@sourceui,''),
SAB = NULLIF(@sab,''),
SVER = NULLIF(@sver,''),
CHANGETYPE = NULLIF(@changetype,''),
CHANGEKEY = NULLIF(@changekey,''),
CHANGEVAL = NULLIF(@changeval,''),
REASON = NULLIF(@reason,''),
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRMAP.RRF' into table MRMAP fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@mapsetcui,@mapsetsab,@mapsubsetid,@maprank,@mapid,@mapsid,@fromid,@fromsid,@fromexpr,@fromtype,@fromrule,@fromres,@rel,@rela,@toid,@tosid,@toexpr,@totype,@torule,@tores,@maprule,@mapres,@maptype,@mapatn,@mapatv,@cvf)
SET MAPSETCUI = @mapsetcui,
MAPSETSAB = @mapsetsab,
MAPSUBSETID = NULLIF(@mapsubsetid,''),
MAPRANK = NULLIF(@maprank,''),
MAPID = @mapid,
MAPSID = NULLIF(@mapsid,''),
FROMID = @fromid,
FROMSID = NULLIF(@fromsid,''),
FROMEXPR = @fromexpr,
FROMTYPE = @fromtype,
FROMRULE = NULLIF(@fromrule,''),
FROMRES = NULLIF(@fromres,''),
REL = @rel,
RELA = NULLIF(@rela,''),
TOID = @toid,
TOSID = NULLIF(@tosid,''),
TOEXPR = @toexpr,
TOTYPE = @totype,
TORULE = NULLIF(@torule,''),
TORES = NULLIF(@tores,''),
MAPRULE = NULLIF(@maprule,''),
MAPRES = NULLIF(@mapres,''),
MAPTYPE = @maptype,
MAPATN = NULLIF(@mapatn,''),
MAPATV = NULLIF(@mapatv,''),
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRRANK.RRF' into table MRRANK fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@mrrank_rank,@sab,@tty,@suppress)
SET MRRANK_RANK = @mrrank_rank,
SAB = @sab,
TTY = @tty,
SUPPRESS = @suppress;

load data local infile '@filePath/MRREL.RRF' into table MRREL fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@cui1,@aui1,@stype1,@rel,@cui2,@aui2,@stype2,@rela,@rui,@srui,@sab,@sl,@rg,@dir,@suppress,@cvf)
SET CUI1 = @cui1,
AUI1 = NULLIF(@aui1,''),
STYPE1 = @stype1,
REL = @rel,
CUI2 = @cui2,
AUI2 = NULLIF(@aui2,''),
STYPE2 = @stype2,
RELA = NULLIF(@rela,''),
RUI = @rui,
SRUI = NULLIF(@srui,''),
SAB = @sab,
SL = @sl,
RG = NULLIF(@rg,''),
DIR = NULLIF(@dir,''),
SUPPRESS = @suppress,
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRSAB.RRF' into table MRSAB fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@vcui,@rcui,@vsab,@rsab,@son,@sf,@sver,@vstart,@vend,@imeta,@rmeta,@slc,@scc,@srl,@tfr,@cfr,@cxty,@ttyl,@atnl,@lat,@cenc,@curver,@sabin,@ssn,@scit)
SET VCUI = NULLIF(@vcui,''),
RCUI = @rcui,
VSAB = @vsab,
RSAB = @rsab,
SON = @son,
SF = @sf,
SVER = NULLIF(@sver,''),
VSTART = NULLIF(@vstart,''),
VEND = NULLIF(@vend,''),
IMETA = @imeta,
RMETA = NULLIF(@rmeta,''),
SLC = NULLIF(@slc,''),
SCC = NULLIF(@scc,''),
SRL = @srl,
TFR = NULLIF(@tfr,''),
CFR = NULLIF(@cfr,''),
CXTY = NULLIF(@cxty,''),
TTYL = NULLIF(@ttyl,''),
ATNL = NULLIF(@atnl,''),
LAT = NULLIF(@lat,''),
CENC = @cenc,
CURVER = @curver,
SABIN = @sabin,
SSN = @ssn,
SCIT = @scit;

load data local infile '@filePath/MRSAT.RRF' into table MRSAT fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@cui,@lui,@sui,@metaui,@stype,@code,@atui,@satui,@atn,@sab,@atv,@suppress,@cvf)
SET CUI = @cui,
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,''),
METAUI = NULLIF(@metaui,''),
STYPE = @stype,
CODE = NULLIF(@code,''),
ATUI = @atui,
SATUI = NULLIF(@satui,''),
ATN = @atn,
SAB = @sab,
ATV = @atv,
SUPPRESS = @suppress,
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRSMAP.RRF' into table MRSMAP fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@mapsetcui,@mapsetsab,@mapid,@mapsid,@fromexpr,@fromtype,@rel,@rela,@toexpr,@totype,@cvf)
SET MAPSETCUI = @mapsetcui,
MAPSETSAB = @mapsetsab,
MAPID = @mapid,
MAPSID = NULLIF(@mapsid,''),
FROMEXPR = @fromexpr,
FROMTYPE = @fromtype,
REL = @rel,
RELA = NULLIF(@rela,''),
TOEXPR = @toexpr,
TOTYPE = @totype,
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRSTY.RRF' into table MRSTY fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@cui,@tui,@stn,@sty,@atui,@cvf)
SET CUI = @cui,
TUI = @tui,
STN = @stn,
STY = @sty,
ATUI = @atui,
CVF = NULLIF(@cvf,'');

load data local infile '@filePath/MRXNS_ENG.RRF' into table MRXNS_ENG fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@nstr,@cui,@lui,@sui)
SET LAT = @lat,
NSTR = @nstr,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXNW_ENG.RRF' into table MRXNW_ENG fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@nwd,@cui,@lui,@sui)
SET LAT = @lat,
NWD = @nwd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRAUI.RRF' into table MRAUI fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@aui1,@cui1,@ver,@rel,@rela,@mapreason,@aui2,@cui2,@mapin)
SET AUI1 = @aui1,
CUI1 = @cui1,
VER = @ver,
REL = NULLIF(@rel,''),
RELA = NULLIF(@rela,''),
MAPREASON = @mapreason,
AUI2 = @aui2,
CUI2 = @cui2,
MAPIN = @mapin;

load data local infile '@filePath/MRXW_BAQ.RRF' into table MRXW_BAQ fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_CHI.RRF' into table MRXW_CHI fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/MRXW_CZE.RRF' into table MRXW_CZE fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/MRXW_DAN.RRF' into table MRXW_DAN fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_DUT.RRF' into table MRXW_DUT fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_ENG.RRF' into table MRXW_ENG fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_EST.RRF' into table MRXW_EST fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/MRXW_FIN.RRF' into table MRXW_FIN fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_FRE.RRF' into table MRXW_FRE fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_GER.RRF' into table MRXW_GER fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_GRE.RRF' into table MRXW_GRE fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/MRXW_HEB.RRF' into table MRXW_HEB fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_HUN.RRF' into table MRXW_HUN fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_ITA.RRF' into table MRXW_ITA fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_JPN.RRF' into table MRXW_JPN fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/MRXW_KOR.RRF' into table MRXW_KOR fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/MRXW_LAV.RRF' into table MRXW_LAV fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/MRXW_NOR.RRF' into table MRXW_NOR fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_POL.RRF' into table MRXW_POL fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_POR.RRF' into table MRXW_POR fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_RUS.RRF' into table MRXW_RUS fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/MRXW_SCR.RRF' into table MRXW_SCR fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/MRXW_SPA.RRF' into table MRXW_SPA fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_SWE.RRF' into table MRXW_SWE fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = @lat,
WD = @wd,
CUI = @cui,
LUI = @lui,
SUI = @sui;

load data local infile '@filePath/MRXW_TUR.RRF' into table MRXW_TUR fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lat,@wd,@cui,@lui,@sui)
SET LAT = NULLIF(@lat,''),
WD = NULLIF(@wd,''),
CUI = NULLIF(@cui,''),
LUI = NULLIF(@lui,''),
SUI = NULLIF(@sui,'');

load data local infile '@filePath/AMBIGSUI.RRF' into table AMBIGSUI fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@sui,@cui)
SET SUI = @sui,
CUI = @cui;

load data local infile '@filePath/AMBIGLUI.RRF' into table AMBIGLUI fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@lui,@cui)
SET LUI = @lui,
CUI = @cui;

load data local infile '@filePath/CHANGE/DELETEDCUI.RRF' into table DELETEDCUI fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@pcui,@pstr)
SET PCUI = @pcui,
PSTR = @pstr;

load data local infile '@filePath/CHANGE/DELETEDLUI.RRF' into table DELETEDLUI fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@plui,@pstr)
SET PLUI = @plui,
PSTR = @pstr;

load data local infile '@filePath/CHANGE/DELETEDSUI.RRF' into table DELETEDSUI fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@psui,@lat,@pstr)
SET PSUI = @psui,
LAT = @lat,
PSTR = @pstr;

load data local infile '@filePath/CHANGE/MERGEDCUI.RRF' into table MERGEDCUI fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@pcui,@cui)
SET PCUI = @pcui,
CUI = @cui;

load data local infile '@filePath/CHANGE/MERGEDLUI.RRF' into table MERGEDLUI fields terminated by '|' ESCAPED BY '' lines terminated by @LINE_TERMINATION@
(@plui,@lui)
SET PLUI = NULLIF(@plui,''),
LUI = NULLIF(@lui,'');