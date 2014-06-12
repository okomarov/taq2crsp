%% Match names/symbol and unique ID
% This scripts performs TAQ 2 CRSP matching through symbol and name using 
% the Levenshtein literal distance and proceeds to creating a unique ID

% Known issues:
% - The literal matches are defined by a threshold for the number of char 
%   changes (additions, deletions and modifications).
% - There's no easy way to identify the root of the symbols with suffixes
%   except for an exhaustive hard-coding. I set to check first letter only.

%% Retrieve data from database
addpath .\utils\
% Connect to db
conn = connect2db();

% Retrieve final match table
curs     = exec(conn,'SELECT * FROM final;');
curs     = fetch(curs);
taq2crsp = curs.data;
taq2crsp = replacedata(taq2crsp, @upper, {'name','symbol'});

% Retrieve crsp stocknames
curs = exec(conn,'SELECT permno, namedt, nameenddt, tsymbol, comnam FROM crsp_msenames;');
curs = fetch(curs);
crsp = curs.data;
crsp = replacedata(crsp, @upper, {'comnam','tsymbol'});

close(curs),clear curs, close(conn), clear conn
% save debugstate
%% Ticker and name match
addpath .\utils\LevenDistance\
load debugstate
N = size(taq2crsp,1);

startdt = fix(crsp.namedt/100);
enddt   = fix(crsp.nameenddt/100);

tic
% SYMBOL and NAME comparison
for ii = 1:size(taq2crsp,1)
    
    % Check that already has permno
    if ~isnan(taq2crsp(ii,:).score), continue, end
    
    % Check if it has name
    name = taq2crsp(ii,:).name{1};
    if isempty(name), continue, end
    
    % From TAQ
    symbol = taq2crsp(ii,:).symbol{1};
    datef  = taq2crsp(ii,:).datef;
        
    % Restrict datef to be in monthly [namedt, nameenddt]
    date  = fix(datef/100);
    idate = date >= startdt & date <= enddt;
    % Create char comparison matrix of symbols that fall within period
    tmp      = crsp(idate,{'tsymbol','comnam','permno'});
    ticklen  = cellfun('size',tmp.tsymbol,2);
    ctsymbol = char(tmp.tsymbol);
    
    % Check letter by letter
    n        = min(numel(symbol), size(ctsymbol,2)); 
    nchars   = sum(bsxfun(@eq, ctsymbol(1:n), symbol(1:n)),2);
    maxchars = max(nchars);
    % Ensure full tsymbol match (can be substring of symbol)
    imatch   = nchars == maxchars & ticklen == maxchars;
    
    % Add name comparison
    comnames = char(tmp.comnam(imatch));
    nnames   = nnz(imatch);
    d        = inf(nnames,1);
    for jj = 1:nnames
        d(jj) = LevenDistance(name, comnames(jj,:));
    end
    iname = d == min(d) & d < 10;
    % If unique name matched
    if nnz(iname) == 1
        % First, expand back through cusip
        pmatch = find(imatch);
        cusip  = taq2crsp.cusip(ii);
        if ~isempty(cusip)
            idx = ismember(taq2crsp.cusip, cusip);
            taq2crsp.score(idx)  = 32;
            taq2crsp.permno(idx) = tmp.permno(pmatch(iname));
        end
        % Then re-label original match
        taq2crsp.score(ii)  = 30;
        taq2crsp.permno(ii) = tmp.permno(pmatch(iname));
        
        fprintf('Lev dist %d on %d\n',min(d),ii)
        %             disp(char(name,comnames(iname)))
    end
end
toc

tic
% NAME match
for ii = 1:size(taq2crsp,1)
     % Check that already has permno
    if ~isnan(taq2crsp(ii,:).score), continue, end
    
    % Check if it has name
    name = taq2crsp(ii,:).name{1};
    if isempty(name), continue, end
	        
    % Restrict datef to be in monthly [namedt, nameenddt]
    datef = taq2crsp(ii,:).datef;
    date  = fix(datef/100);
    idate = date >= startdt & date <= enddt;
    
    % Temporary
    tmp      = crsp(idate,{'tsymbol','comnam','permno'});
    comnames = char(tmp.comnam);
    nnames   = nnz(idate);
    d        = inf(nnames,1);
    for jj = 1:nnames
        d(jj) = LevenDistance(name, comnames(jj,:));
    end
    
    iname = d == min(d) & d < 10;
    % If unique name matched
    if nnz(iname) == 1
        % First, expand back through cusip
        cusip = taq2crsp.cusip(ii);
        if ~isempty(cusip)
            idx = ismember(taq2crsp.cusip, cusip);
            taq2crsp.score(idx)  = 42;
            taq2crsp.permno(idx) = tmp.permno(iname);
        end
        % Then re-label original match
        taq2crsp.score(ii)  = 40;
        taq2crsp.permno(ii) = tmp.permno(iname);
        
        fprintf('Lev dist %d on %d\n',min(d),ii)
        %             disp(char(name,comnames(iname)))
    end
end
toc
% save debugstate2 taq2crsp crsp

%% Unique ID
load debugstate2

% Set starting value of ID
ID = 0;
taq2crsp.ID = NaN(size(taq2crsp,1),1);

% If only symbol tag by that independently of the date 
% (ensure that symbols retrieved here don't have a match on the ones with cusip/permno)
idx = isnan(taq2crsp.permno) & cellfun('isempty',taq2crsp.cusip);
onlySymbol = unique(taq2crsp.symbol(idx));

while ~isempty(onlySymbol)
    ID     = ID+1;
    symbol = onlySymbol(1);
    taq2crsp.ID(strcmpi(symbol, taq2crsp.symbol) & idx) = ID;
    onlySymbol = onlySymbol(2:end);
end
tic

% If has also CUSIP but not PERMNO
taq2crsp.symlen = cellfun('size',taq2crsp.symbol,2);
taq2crsp  = sortrows(taq2crsp,{'cusip','datef'});
onlyCusip = unique(taq2crsp.cusip(isnan(taq2crsp.permno) & ~cellfun('isempty',taq2crsp.cusip) &...
                   isnan(taq2crsp.ID)));
while ~isempty(onlyCusip)
    ID       = ID+1;
    cusip    = onlyCusip(1);
    icusip   = strcmpi(cusip, taq2crsp.cusip);
    % Work backwards to retrieve by permno if any matched record has it
    tmp   = taq2crsp(icusip,:);
    icomb = ismember(taq2crsp.permno, tmp.permno) | icusip;
    tmp   = taq2crsp(icomb,:);
                   
    nmatches = nnz(icomb);
    if  nmatches == 1 
        taq2crsp.ID(icomb) = ID;
    else

        pos = find(icomb);
        
        % Initialize reference symbol
        refID  = ID;
        refSym = tmp.symbol{1};
        refN   = tmp.symlen(1);
               
        % Match reference symbol
        idx         = strcmpi(refSym, tmp.symbol);
        tmp.ID(idx) = refID;
        
        % LOOP for all remaining records
        for ii = 2:nmatches
            
            % If already has ID, skip
            if ~isnan(tmp.ID(ii)), continue, end
            
            curr  = tmp.symbol{ii};
            ncurr = tmp.symlen(ii);
            
            % REF substr of CURR
            if strncmpi(refSym, curr, refN)
                ID          = ID + 1;
                idx         = strcmpi(curr, tmp.symbol);
                tmp.ID(idx) = ID;
            
            % CURR substr of REF
            elseif strncmpi(curr, refSym, ncurr)
                [refID, ID] = deal(ID + 1);
                idx         = strcmpi(curr, tmp.symbol);
                tmp.ID(idx) = refID;
                refSym      = curr;
                refN        = tmp.symlen(ii);
            else
                % Check that periods don't overlap
                refDates  = tmp.datef(strcmpi(refSym,tmp.symbol));
                currDates = tmp.datef(strcmpi(curr,tmp.symbol));
                noOverlap = sum(bsxfun(@ge, refDates, currDates'));
                noOverlap = all(noOverlap == 0 | noOverlap == numel(refDates));

                % [WEAK LINK: how to identify root] No common root
                if noOverlap && ...
                    ( (refN < 4 &&      refSym(1)   ~= curr(1)   ) || ...
                      (refN > 3 && ~all(refSym(1:2) ~= curr(1:2)))   )
                    idx         = strcmpi(curr, tmp.symbol);
                    tmp.ID(idx) = refID;
                else
                    ID          = ID + 1;
                    idx         = strcmpi(curr, tmp.symbol);
                    tmp.ID(idx) = ID;
                end
                refSym = curr;
                refN   = tmp.symlen(ii);
            end
        end % LOOP
        % Assign back to original array
        taq2crsp.ID(pos) = tmp.ID;
    end
    onlyCusip = onlyCusip(2:end);
end
toc                    

% Has PERMNO
taq2crsp   = sortrows(taq2crsp,{'permno','datef'});
permnos = unique(taq2crsp.permno(~isnan(taq2crsp.permno) & isnan(taq2crsp.ID)));
while ~isempty(permnos)
    ID       = ID+1;
    permno   = permnos(1);
    ipermno  = taq2crsp.permno == permno;
    nmatches = nnz(ipermno);
     
    if nmatches == 1
        taq2crsp.ID(ipermno) = ID;
    else
        % Retrieve records with given PERMNO
        tmp = taq2crsp(ipermno,:);
        pos = find(ipermno);
        
        % Initialize reference symbol
        refID  = ID;
        refSym = tmp.symbol{1};
        refN   = tmp.symlen(1);
               
        % Match reference symbol
        idx         = strcmpi(refSym, tmp.symbol);
        tmp.ID(idx) = refID;
        
        % LOOP for all remaining records
        for ii = 2:nmatches
            
            % If already has ID, skip
            if ~isnan(tmp.ID(ii)), continue, end
            
            curr  = tmp.symbol{ii};
            ncurr = tmp.symlen(ii);
            
            % REF substr of CURR
            if strncmpi(refSym, curr, refN)
                ID          = ID + 1;
                idx         = strcmpi(curr, tmp.symbol);
                tmp.ID(idx) = ID;
            
            % CURR substr of REF
            elseif strncmpi(curr, refSym, ncurr)
                [refID, ID] = deal(ID + 1);
                idx         = strcmpi(curr, tmp.symbol);
                tmp.ID(idx) = refID;
                refSym      = curr;
                refN        = tmp.symlen(ii);
            else
                % Check that periods don't overlap
                refDates  = tmp.datef(strcmpi(refSym,tmp.symbol));
                currDates = tmp.datef(strcmpi(curr,tmp.symbol));
                noOverlap = sum(bsxfun(@ge, refDates, currDates'));
                noOverlap = all(noOverlap == 0 | noOverlap == numel(refDates));

                % [WEAK LINK] No common root
                if noOverlap && ...
                    ( (refN < 4 &&      refSym(1)   ~= curr(1)   ) || ...
                      (refN > 3 && ~all(refSym(1:2) ~= curr(1:2)))   )

                    idx         = strcmpi(curr, tmp.symbol);
                    tmp.ID(idx) = refID;
                else
                    ID          = ID + 1;
                    idx         = strcmpi(curr, tmp.symbol);
                    tmp.ID(idx) = ID;
                end
                refSym = curr;
                refN   = tmp.symlen(ii);
            end
        end % LOOP
        % Assign back to original array
        taq2crsp.ID(pos) = tmp.ID;
    end
    permnos = permnos(2:end);
end
toc    
%% Update back to db
conn = connect2db();

tic
cols  = {'ID', 'permno','score'};
data  = [taq2crsp.(cols{1}) taq2crsp.(cols{2}) taq2crsp.(cols{3})];
where = arrayfun(@(x) sprintf('where PK = %d',x), taq2crsp.PK,'un',0);
update(conn, 'final', cols, data, where)
toc

%% Save
save taq2crsp.mat taq2crsp