%% Match names and symbol
% Based on Levenshtein

%% Retrieve data from database
% javaaddpath('C:\Program Files (x86)\MySQL\MySQL Connector J\mysql-connector-java-5.1.30-bin.jar')
% 
% setdbprefs({'DataReturnFormat';'NullStringRead'},{'dataset';''})
% s.dbname = 'hfbetas';
% s.user   = 'okomarov';
% s.driver = 'com.mysql.jdbc.Driver';
% s.dburl  = sprintf('jdbc:mysql://localhost:3306/%s', dbname);
% s.pass   = input('Password: ','s');
% conn   = database(s.dbname, s.user, s.pass, s.driver, s.dburl);
% clear pass
% clc
% if isconnection(conn),fprintf('Connection established\n'), else error('Not connected.'), end
% 
% % Retrieve final
% curs  = exec(conn,'SELECT * FROM final;');
% curs  = fetch(curs);
% final = curs.data;
% final = replacedata(final, @upper, {'name','symbol'});
% 
% % Retrieve crsp stocknames
% curs = exec(conn,'SELECT permno, namedt, nameenddt, tsymbol, comnam FROM crsp_msenames;');
% curs = fetch(curs);
% crsp = curs.data;
% crsp = replacedata(crsp, @upper, {'comnam','tsymbol'});
% 
% close(curs),clear curs, close(conn), clear conn
% save debugstate
%% Ticker and name match
addpath .\utils\LevenDistance\
load debugstate
N = size(final,1);

startdt = fix(crsp.namedt/100);
enddt   = fix(crsp.nameenddt/100);

tic
% SYMBOL and NAME comparison
for ii = 1:size(final,1)
    
    % Check that already has permno
    if ~isnan(final(ii,:).score), continue, end
    
    % Check if it has name
    name = final(ii,:).name{1};
    if isempty(name), continue, end
    
    % From TAQ
    symbol = final(ii,:).symbol{1};
    datef  = final(ii,:).datef;
        
    % Restrict datef to be in monthly [namedt, nameenddt]
    date  = fix(datef/100);
    idate = date >= startdt & date <= enddt;
    % Create char comparison matrix
    tmp      = crsp(idate,{'tsymbol','comnam','permno'});
    ticklen  = cellfun('size',tmp.tsymbol,2);
    ctsymbol = char(tmp.tsymbol);
    nsym     = numel(symbol);
    ntsym    = size(ctsymbol,2);
    
    if nsym > ntsym
        symbol = symbol(1:ntsym);
    else
        ctsymbol = ctsymbol(:,1:nsym);
    end
    
    % Check letter by letter
    nchars   = sum(bsxfun(@eq, ctsymbol, symbol),2);
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
    iname = d == min(d) & d < 15;
    % If unique name matched
    if nnz(iname) == 1
        pmatch = find(imatch);
        final(ii,:).score = 30;
        final(ii,:).permno = tmp.permno(pmatch(iname));
        fprintf('Lev dist %d on %d\n',min(d),ii)
        %             disp(char(name,comnames(iname)))
    end
end
toc

tic
% NAME match
for ii = 1:size(final,1)
     % Check that already has permno
    if ~isnan(final(ii,:).score), continue, end
    
    % Check if it has name
    name = final(ii,:).name{1};
    if isempty(name), continue, end
	        
    % Restrict datef to be in monthly [namedt, nameenddt]
    datef = final(ii,:).datef;
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
        final(ii,:).score = 40;
        final(ii,:).permno = tmp.permno(iname);
        fprintf('Lev dist %d on %d\n',min(d),ii)
        %             disp(char(name,comnames(iname)))
    end
end
toc
%% Update back to db
javaaddpath('C:\Program Files (x86)\MySQL\MySQL Connector J\mysql-connector-java-5.1.30-bin.jar')

% Establish connection
s.dbname = 'hfbetas';
s.user   = 'okomarov';
s.driver = 'com.mysql.jdbc.Driver';
s.dburl  = sprintf('jdbc:mysql://localhost:3306/%s', dbname);
s.pass   = input('Password: ','s');
conn     = database(s.dbname, s.user, s.pass, s.driver, s.dburl);
clear s
clc
if isconnection(conn),fprintf('Connection established\n'), else error('Not connected.'), end

tic
idx   = final.score == 30 | final.score == 40;
cols  = {'permno','score'};
data  = [final.(cols{1})(idx) final.(cols{2})(idx)];
where = arrayfun(@(x) sprintf('where PK = %d',x), final.PK(idx),'un',0);
update(conn, 'final', cols, data, where)
toc

%% Unique ID



