%% TAQmaster for MySQL
d = '.\data';
%% Load .csv
% List all TAQ .csv master files
list      = unzip(fullfile(d,'raw','TAQmast.csv.zip'),d);
list      = sort(list);
% Due to a change in FDATE as per TAQ readme.txt which screwed the field,exclude 2000/02 and 2000/03 master files
idx       = cellfun('isempty', regexp(list,'(mst200002|mst200003)','once'));
delete(list{~idx});
list      = list(idx);
nfiles    = numel(list);
TAQmaster = cell(nfiles,1);
% fmt       = ['%s %s %s ',repmat('%*u8 ',1,9),'%*s %*f %*f %*s %*u8 %f %*[^\n]'];
fmt       = ['%s %s %s ',repmat('%u8 ',1,9),'%s %*f %*f %s %u8 %f %*[^\n]'];
opts      = {'Delimiter',',','ReadVarNames',1,'CommentStyle',{'"','"'}};

% LOOP by file - 20 sec
for ii = 1:nfiles
    disp(list{ii})
    % Read in the whole master file
    TAQmaster{ii} = dataset('File',list{ii}, 'Format',fmt, opts{:});
    % Eventually rename DATEF to FDATE (in some files it changes)
    TAQmaster{ii}.Properties.VarNames = regexprep(TAQmaster{ii}.Properties.VarNames,'(?i)datef','FDATE');
end
delete(list{:})
%% Load .tab 
% Add .tab master files from dvd
list2     = unzip(fullfile(d,'raw','TAQmast.tab.zip'),d);
list2     = sort(list2);
fmt       = '%10c %30c %12c %c%c%c %*2c %c%c%c%c%c %c %4c %*10c %*4c %c %c %8c %*[^\n]';
names     = {'SYMBOL','NAME','CUSIP','ETN','ETA','ETB','ETP','ETX','ETT','ETO','ETW','ITS','ICODE','DENOM','TYPE','FDATE'};
nfiles2   = numel(list2);
TAQmaster = [TAQmaster; cell(nfiles2,1)];

% LOOP by file - 20 sec
tic
for ii = 1:nfiles2
    disp(list2{ii})
    
    % Read in fixed width master files in a temporary variable
    fid = fopen(list2{ii});
    tmp = textscan(fid, fmt,'Whitespace','');
    fclose(fid);
    
    % Convert
    f = @(x,n) cellstr(x{n});
    g = @(x) [f(x,1) f(x,2) f(x,3)  num2cell(uint8([x{4:12}]-'0'))  f(x,13) f(x,14) num2cell(uint8(x{15}-'0')) num2cell(str2num(x{16}))];
    TAQmaster{nfiles+ii} = cell2dataset(g(tmp),'VarNames',names);
end
toc
delete(list2{:})
%% Post-processing
% NOTE: I decided to avoid additional post-processing on the symbols here,
% and tackle the multiple SYMBOLs per CUSIP at 'same' FDATE later, after
% the match with PERMNO has been established. 
% To understand why this comment is relevant we need to answer to the question
% "How does the multiple-SYMBOL-same-FDATE problem affect us?"
% If I query by permno, I will most likely get two or more overlapping
% price series matched back to TAQ. Also, I cannot edit SYMBOL here to get
% rid of the secondary series because I want to retain a clean mapping. 
% Thus, after the mapping has been carried out, I will tag with a unique ID
% that will into consideration PERMNO, SYMBOL and FDATE.
% It's actually more complicated than it might appear, due to the numerous
% suffixes that SYMBOLS have, see the TAQ monthly guide suffix appendices.

% Concatenate everything - 20 sec
TAQmaster = cat(1,TAQmaster{:});

% Extract CUSIP info. CUSIP lengths can be 12 (full), 0 (absent) and 9 (missing NSCC issue digits)
CUSIP = char(TAQmaster.CUSIP);

% Extract 8-CUSIP
TAQmaster.CUSIP8 = cellstr(CUSIP(:,1:8));

% Extract the 9th digit/character, i.e. 3rd of the TAQ issue
tmp = textscan([CUSIP(:,9),repmat(',',size(CUSIP,1),1)]','%f','Delimiter',',');
TAQmaster.CUSIP9 = tmp{1};

% Extract the last 3 digits, i.e. the NSCC issue:
% NYSE 000, NYSE at issue 100
% MKT  001, MKT  at issue 101
% NASD 002, NASD at issue 102
tmp = textscan([CUSIP(:,10:12),repmat(',',size(CUSIP,1),1)]','%f','Delimiter',',');
TAQmaster.NSCC = tmp{1};

% All zero 8-CUSIPs
idx = strcmp(TAQmaster.CUSIP8,'00000000');
% prob        = TAQmaster(idx,:);
% [un,~,subs] = unique(prob(:,{'SYMBOL','NAME','CUSIP8'}));
% un.FDATE    = accumarray(subs, prob.FDATE,[],@min);
% Make '00000000' 8-cusips empty
TAQmaster.CUSIP8(idx) = {''};
%% TAQsymbols

% Export symbols only, necessary for cleanest match with CRSP cusip 
TAQsymbols = unique(TAQmaster(:,{'CUSIP8','SYMBOL','NAME','FDATE'}));

% Collapse dates for symbols, NOTE the sorting. This is to capure forth/back in the symbol:
% 00088E10	IATV	ACTV INC	19930104
% 00088E10	ACTV	ACTV INC	19950503
% 00088E10	IATV	ACTV INC	19980630
TAQsymbols = sortrows(TAQsymbols,{'CUSIP8','FDATE','SYMBOL'});
idx        =  [true; ~(strcmpi(TAQsymbols.CUSIP8(2:end), TAQsymbols.CUSIP8(1:end-1)) &...
                       strcmpi(TAQsymbols.SYMBOL(2:end), TAQsymbols.SYMBOL(1:end-1)))];
TAQsymbols = TAQsymbols(idx,:);

% Replace missing with \N, for MySQL's LOAD INFILE
missing = ismissing(TAQsymbols);
TAQsymbols.NAME(missing(:,3)) = {'\N'};
TAQsymbols.CUSIP8(missing(:,1)) = {'\N'};

% Export
export(TAQsymbols,'file',fullfile(d,'TAQsymbols.tab'),'Delim','\t') % Remember to change manually to UTF8 encoding
%% TAQcodetype
TAQcodetype = unique(TAQmaster(:,{'CUSIP8','SYMBOL','FDATE','ICODE','TYPE'}));

TAQcodetype = sortrows(TAQcodetype,{'CUSIP8','FDATE','SYMBOL'});
idx         =  [true; ~(strcmpi(TAQcodetype.CUSIP8 (2:end), TAQcodetype.CUSIP8 (1:end-1)) &...
                        strcmpi(TAQcodetype.SYMBOL(2:end), TAQcodetype.SYMBOL(1:end-1)) &...
                        strcmpi(TAQcodetype.ICODE (2:end), TAQcodetype.ICODE (1:end-1)) &...
                                TAQcodetype.TYPE  (2:end)==TAQcodetype.TYPE  (1:end-1) )];
TAQcodetype = TAQcodetype(idx,:);

% Replace missing with \N, for MySQL's LOAD INFILE
missing = ismissing(TAQcodetype);
TAQcodetype.CUSIP8(missing(:,1)) = {'\N'};
TAQcodetype.ICODE(missing(:,4)) = {'\N'};
export(TAQcodetype,'file',fullfile(d,'TAQcodetype.tab'),'Delim','\t') % Remember to change manually to UTF8 encoding
toc
%% WRDStclink
% Import .csv
addpath .\utils\
d          = '.\data';
list       = unzip(fullfile(d,'raw','WRDStclink.csv.zip'),d);
fid        = fopen(list{:});
varnames   = textscan(fid,'%s %s %s %s %s',1,'delimiter',',','CollectOutput',true);
varnames   = varnames{:};
WRDStclink = textscan(fid,'"%f" %q %q %q "%f" %*[^\n]','Delimiter',',','Headerlines',1);
fclose(fid);
WRDStclink = dataset({WRDStclink{1}, varnames{1}},...
                     {WRDStclink{2}, varnames{2}},...
                     {WRDStclink{3}, varnames{3}},...
                     {WRDStclink{4}, varnames{4}},...
                     {WRDStclink{5}, varnames{5}});
WRDStclink = replacedata(WRDStclink, @(x) serial2yyyymmdd(datenum(x,'yyyy-mm-dd')),'DATE');

% Consolidate dates
WRDStclink = sortrows(WRDStclink,{'PERMNO','CUSIP','DATE','SYMBOL'});
idx        =  [true; ~(        WRDStclink.PERMNO(2:end) == WRDStclink.PERMNO(1:end-1) &...
                       strcmpi(WRDStclink.CUSIP (2:end), WRDStclink.CUSIP(1:end-1)) &...
                       strcmpi(WRDStclink.SYMBOL(2:end), WRDStclink.SYMBOL(1:end-1)))];
WRDStclink = WRDStclink(idx,:);

% Export
export(WRDStclink,'file',fullfile(d,'WRDStclink.tab'),'Delim','\t') % Remember to change manually to UTF8 encoding
delete(list{:})