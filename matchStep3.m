% %% Match names and symbol
% % Based on Levenshtein
% 
% %% Retrieve data from database
% javaaddpath('C:\Program Files (x86)\MySQL\MySQL Connector J\mysql-connector-java-5.1.30-bin.jar')
% 
% setdbprefs({'DataReturnFormat';'NullStringRead'},{'dataset';''})
% s.dbname = 'hfbetas';
% s.user   = 'okomarov';
% s.driver = 'com.mysql.jdbc.Driver';
% s.dburl  = sprintf('jdbc:mysql://localhost:3306/%s', s.dbname);
% s.pass   = input('Password: ','s');
% conn     = database(s.dbname, s.user, s.pass, s.driver, s.dburl);
% clear s
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
% %% Ticker and name match
% addpath .\utils\LevenDistance\
% load debugstate
% N = size(final,1);
% 
% startdt = fix(crsp.namedt/100);
% enddt   = fix(crsp.nameenddt/100);
% 
% tic
% % SYMBOL and NAME comparison
% for ii = 1:size(final,1)
%     
%     % Check that already has permno
%     if ~isnan(final(ii,:).score), continue, end
%     
%     % Check if it has name
%     name = final(ii,:).name{1};
%     if isempty(name), continue, end
%     
%     % From TAQ
%     symbol = final(ii,:).symbol{1};
%     datef  = final(ii,:).datef;
%         
%     % Restrict datef to be in monthly [namedt, nameenddt]
%     date  = fix(datef/100);
%     idate = date >= startdt & date <= enddt;
%     % Create char comparison matrix
%     tmp      = crsp(idate,{'tsymbol','comnam','permno'});
%     ticklen  = cellfun('size',tmp.tsymbol,2);
%     ctsymbol = char(tmp.tsymbol);
%     nsym     = numel(symbol);
%     ntsym    = size(ctsymbol,2);
%     
%     if nsym > ntsym
%         symbol = symbol(1:ntsym);
%     else
%         ctsymbol = ctsymbol(:,1:nsym);
%     end
%     
%     % Check letter by letter
%     nchars   = sum(bsxfun(@eq, ctsymbol, symbol),2);
%     maxchars = max(nchars);
%     % Ensure full tsymbol match (can be substring of symbol)
%     imatch   = nchars == maxchars & ticklen == maxchars;
%     
%     % Add name comparison
%     comnames = char(tmp.comnam(imatch));
%     nnames   = nnz(imatch);
%     d        = inf(nnames,1);
%     for jj = 1:nnames
%         d(jj) = LevenDistance(name, comnames(jj,:));
%     end
%     iname = d == min(d) & d < 10;
%     % If unique name matched
%     if nnz(iname) == 1
%         pmatch = find(imatch);
%         final(ii,:).score = 30;
%         final(ii,:).permno = tmp.permno(pmatch(iname));
%         fprintf('Lev dist %d on %d\n',min(d),ii)
%         %             disp(char(name,comnames(iname)))
%     end
% end
% toc
% 
% tic
% % NAME match
% for ii = 1:size(final,1)
%      % Check that already has permno
%     if ~isnan(final(ii,:).score), continue, end
%     
%     % Check if it has name
%     name = final(ii,:).name{1};
%     if isempty(name), continue, end
% 	        
%     % Restrict datef to be in monthly [namedt, nameenddt]
%     datef = final(ii,:).datef;
%     date  = fix(datef/100);
%     idate = date >= startdt & date <= enddt;
%     
%     % Temporary
%     tmp      = crsp(idate,{'tsymbol','comnam','permno'});
%     comnames = char(tmp.comnam);
%     nnames   = nnz(idate);
%     d        = inf(nnames,1);
%     for jj = 1:nnames
%         d(jj) = LevenDistance(name, comnames(jj,:));
%     end
%     
%     iname = d == min(d) & d < 10;
%     % If unique name matched
%     if nnz(iname) == 1
%         final(ii,:).score = 40;
%         final(ii,:).permno = tmp.permno(iname);
%         fprintf('Lev dist %d on %d\n',min(d),ii)
%         %             disp(char(name,comnames(iname)))
%     end
% end
% toc
% save debugstate2 final crsp
% %% Update back to db
% javaaddpath('C:\Program Files (x86)\MySQL\MySQL Connector J\mysql-connector-java-5.1.30-bin.jar')
% 
% % Establish connection
% s.dbname = 'hfbetas';
% s.user   = 'okomarov';
% s.driver = 'com.mysql.jdbc.Driver';
% s.dburl  = sprintf('jdbc:mysql://localhost:3306/%s', s.dbname);
% s.pass   = input('Password: ','s');
% conn     = database(s.dbname, s.user, s.pass, s.driver, s.dburl);
% clear s
% clc
% if isconnection(conn),fprintf('Connection established\n'), else error('Not connected.'), end
% 
% tic
% idx   = final.score == 30 | final.score == 40;
% cols  = {'permno','score'};
% data  = [final.(cols{1})(idx) final.(cols{2})(idx)];
% where = arrayfun(@(x) sprintf('where PK = %d',x), final.PK(idx),'un',0);
% update(conn, 'final', cols, data, where)
% toc
% 
%% Unique ID
% load debugstate2

% % Set starting value of ID
% ID = 0;
% 
% % If only symbol tag by that independently of the date 
% % (ensure that symbols retrieved here don't have a match on the ones with cusip/permno)
% onlySymbol = unique(final.symbol(isnan(final.permno) & cellfun('isempty',final.cusip)));
% 
% while ~isempty(onlySymbol)
%     ID     = ID+1;
%     symbol = onlySymbol(1);
%     final.ID(strcmpi(symbol, final.symbol)) = ID;
%     onlySymbol = onlySymbol(2:end);
% end

load debugstate3
tic
% If has also CUSIP but not PERMNO
final = sortrows(final,{'cusip','datef'});
final.symlen = cellfun('size',final.symbol,2);
onlyCusip = unique(final.cusip(isnan(final.permno) & ~cellfun('isempty',final.cusip) &...
                   isnan(final.ID)));
while ~isempty(onlyCusip)
    ID       = ID+1;
    cusip    = onlyCusip(1);
    icusip   = strcmpi(cusip, final.cusip);
    nmatches = nnz(icusip);
     
    if  nmatches == 1 
        final.ID(icusip) = ID;
    else
        % Retrieve records with given CUSIP
        tmp = final(icusip,:);
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
        final.ID(pos) = tmp.ID;
    end
    onlyCusip = onlyCusip(2:end);
end
toc                    

% Has PERMNO
final   = sortrows(final,{'permno','datef'});
permnos = unique(final.permno(~isnan(final.permno) & isnan(final.ID)));
while ~isempty(permnos)
    ID       = ID+1;
    permno   = permnos(1);
    ipermno  = final.permno == permno;
    nmatches = nnz(ipermno);
     
    if nmatches == 1
        final.ID(ipermno) = ID;
    else
        % Retrieve records with given CUSIP
        tmp = final(ipermno,:);
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
        final.ID(pos) = tmp.ID;
    end
    permnos = permnos(2:end);
end
toc    