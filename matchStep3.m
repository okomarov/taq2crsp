%% Match names and symbol
% Based on Levenshtein

%% Retrieve data from database
javaaddpath('C:\Program Files (x86)\MySQL\MySQL Connector J\mysql-connector-java-5.1.30-bin.jar')

setdbprefs({'DataReturnFormat';'NullStringRead'},{'dataset';''})
dbname = 'hfbetas';
user   = 'okomarov';
driver = 'com.mysql.jdbc.Driver';
dburl  = sprintf('jdbc:mysql://localhost:3306/%s', dbname);
pass   = input('Password: ','s');
conn   = database(dbname, user, pass, driver, dburl);
clear pass
clc
if ~isconnection(conn), error('Not connected.'), end

% Retrieve final
curs  = exec(conn,'SELECT pk, permno, symbol, name, datef FROM final WHERE score is null;');
curs  = fetch(curs);
final = curs.data;
final = replacedata(final, @upper, {'name','symbol'});

% Retrieve crsp stocknames
curs = exec(conn,'SELECT permno, namedt, nameenddt, ticker, comnam FROM crsp_stocknames;');
curs = fetch(curs);
crsp = curs.data;
crsp = replacedata(crsp, @upper, {'comnam','ticker'});

close(curs),clear curs, close(conn), clear conn
save debugstate
%% Ticker and name match
addpath .\utils\edit_distances\
load debugstate
N = size(final,1);
final.score = NaN(N,1);

startdt = fix(crsp.namedt/100);
enddt   = fix(crsp.nameenddt/100);
tic
for ii = 1:size(final,1)
    % From TAQ
    symbol = final(ii,:).symbol{1};
    n      = numel(symbol);
    
    % Check against first 5 letters, CRSP tickers are never longer
    if n > 5, symbol = symbol(1:5); n = 5; end
    
    datef  = final(ii,:).datef;
    name   = final(ii,:).name{1};
    
    
    % Restrict datef to be in monthly [namedt, nameenddt]
    date  = fix(datef/100);
    idate = date >= startdt & date <= enddt;
    % Create char comparison matrix
    tmp     = crsp(idate,{'ticker','comnam','permno'});
    ticklen = cellfun('size',tmp.ticker,2);
    cticker = char(tmp.ticker);
    
    % Check letter by letter
    nchars   = sum(bsxfun(@eq, cticker(:,1:n), symbol),2);
    maxchars = max(nchars);
    
    imatch = nchars == maxchars & ticklen == maxchars;
    if ~isempty(name)
        comnames = char(tmp.comnam(imatch));
        nnames   = nnz(imatch);
        d = zeros(nnz(imatch),1);
        for jj = 1:nnames
            d(jj) = edit_distance_levenshtein(name, comnames(jj,:));
        end
        iname = d == min(d) & d < 10;
        if nnz(iname) == 1
            pmatch = find(imatch);
            final(ii,:).score = 30;
            final(ii,:).permno = tmp.permno(pmatch(iname));
            fprintf('Lev dist %d on %d\n',min(d),ii) 
        elseif ~isempty(iname)
            fprintf('Multiple or bad name on %d\n',ii) 
        end
    else
        permno = unique(tmp.permno(imatch));
        if numel(permno) > 1
            fprintf('Multiple permnos no name on %d\n',ii)
        elseif ~isempty(permno)
            final(ii,:).score = 40;
            final(ii,:).permno = permno;
        end
    end
end
toc