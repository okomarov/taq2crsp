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
startdt = fix(crsp.namedt/100);
enddt   = fix(crsp.nameenddt/100);
ticklen = cellfun('size',crsp.ticker,2);

for ii = 1:size(final,1)
    symbol = final(ii,:).symbol{1};
    datef  = final(ii,:).datef;
    
    % Has symbol
    if ~isempty(symbol)
        % Restrict datef to be in monthly [namedt, nameenddt]
        date = fix(datef/100);
        idx  = date >= startdt & date <= enddt;
        % Create char comparison matrix
        [unticker,pos] = unique(crsp.ticker(idx));
        unticker = char(unticker);
        
        % Check letter by letter
        n        = numel(symbol);
        nchars   = sum(bsxfun(@eq, unticker(:,1:n), symbol),2);
        maxchars = max(nchars);
        
        if maxchars == n
            disp(1)
        else
            
        end
        
    elseif ~isempty(record.name)

    end

    
    crsp.comnam(1e4)
end
