%% Match names/symbol and unique ID
% This scripts performs TAQ 2 CRSP matching through symbol and name using 
% the Levenshtein literal distance and proceeds to creating a unique ID

% Known issues:
% - The literal matches are defined by a threshold for the number of char 
%   changes (additions, deletions and modifications).
% - There's no easy way to identify the root of the symbols with suffixes
%   except for an exhaustive hard-coding. I set to check first letter only.

%% Retrieve data from database
javaaddpath('C:\Program Files (x86)\MySQL\MySQL Connector J\mysql-connector-java-5.1.30-bin.jar')

setdbprefs({'DataReturnFormat';'NullStringRead'},{'dataset';''})
s.dbname = 'hfbetas';
s.user   = 'okomarov';
s.driver = 'com.mysql.jdbc.Driver';
s.dburl  = sprintf('jdbc:mysql://localhost:3306/%s', s.dbname);
s.pass   = input('Password: ','s');
conn     = database(s.dbname, s.user, s.pass, s.driver, s.dburl);
clear s
clc
if isconnection(conn),fprintf('Connection established\n'), else error('Not connected.'), end

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
    % Create char comparison matrix
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
        pmatch = find(imatch);
        taq2crsp(ii,:).score = 30;
        taq2crsp(ii,:).permno = tmp.permno(pmatch(iname));
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
        taq2crsp(ii,:).score = 40;
        taq2crsp(ii,:).permno = tmp.permno(iname);
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

% If only symbol tag by that independently of the date 
% (ensure that symbols retrieved here don't have a match on the ones with cusip/permno)
onlySymbol = unique(taq2crsp.symbol(isnan(taq2crsp.permno) & cellfun('isempty',taq2crsp.cusip)));

while ~isempty(onlySymbol)
    ID     = ID+1;
    symbol = onlySymbol(1);
    taq2crsp.ID(strcmpi(symbol, taq2crsp.symbol)) = ID;
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
    nmatches = nnz(icusip);
     
    if  nmatches == 1 
        taq2crsp.ID(icusip) = ID;
    else
        % Retrieve records with given CUSIP
        tmp = taq2crsp(icusip,:);
        pos = find(icusip);
        
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
                refID       = ID + 1;
                idx         = strcmpi(curr, tmp.symbol);
                tmp.ID(idx) = refID;
                refSym      = curr;
            else
                % Check that periods don't overlap
                refDates  = tmp.datef(strcmpi(refSym,tmp.symbol));
                currDates = tmp.datef(strcmpi(curr,tmp.symbol));
                noOverlap = sum(bsxfun(@ge, refDates, currDates'));
                noOverlap = all(noOverlap == 0 | noOverlap == numel(refDates));

                % [WEAK LINK: how to identify root] No common root
                if refSym(1) ~= curr(1) && noOverlap
                    idx         = strcmpi(curr, tmp.symbol);
                    tmp.ID(idx) = refID;
                else
                    refID       = ID + 1;
                    idx         = strcmpi(curr, tmp.symbol);
                    tmp.ID(idx) = refID;
                end
                refSym = curr;
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
                refID       = ID + 1;
                idx         = strcmpi(curr, tmp.symbol);
                tmp.ID(idx) = refID;
                refSym      = curr;
            else
                % Check that periods don't overlap
                refDates  = tmp.datef(strcmpi(refSym,tmp.symbol));
                currDates = tmp.datef(strcmpi(curr,tmp.symbol));
                noOverlap = sum(bsxfun(@ge, refDates, currDates'));
                noOverlap = all(noOverlap == 0 | noOverlap == numel(refDates));

                % [WEAK LINK] No common root
                if refSym(1) ~= curr(1) && noOverlap
                    idx         = strcmpi(curr, tmp.symbol);
                    tmp.ID(idx) = refID;
                else
                    refID       = ID + 1;
                    idx         = strcmpi(curr, tmp.symbol);
                    tmp.ID(idx) = refID;
                end
                refSym = curr;
            end
        end % LOOP
        % Assign back to original array
        taq2crsp.ID(pos) = tmp.ID;
    end
    permnos = permnos(2:end);
end
toc    
%% Update back to db
javaaddpath('C:\Program Files (x86)\MySQL\MySQL Connector J\mysql-connector-java-5.1.30-bin.jar')

% Establish connection
s.dbname = 'hfbetas';
s.user   = 'okomarov';
s.driver = 'com.mysql.jdbc.Driver';
s.dburl  = sprintf('jdbc:mysql://localhost:3306/%s', s.dbname);
s.pass   = input('Password: ','s');
conn     = database(s.dbname, s.user, s.pass, s.driver, s.dburl);
clear s
clc
if isconnection(conn),fprintf('Connection established\n'), else error('Not connected.'), end

tic
cols  = {'ID', 'permno','score'};
data  = [taq2crsp.(cols{1}) taq2crsp.(cols{2}) taq2crsp.(cols{2})];
where = arrayfun(@(x) sprintf('where PK = %d',x), taq2crsp.PK,'un',0);
update(conn, 'final', cols, data, where)
toc

%% Save
save taq2crsp.mat taq2crsp