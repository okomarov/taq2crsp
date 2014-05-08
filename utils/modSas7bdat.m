function modSas7bdat(csvfile)

% MODSAS7BDAT Minor modifications to .csv produced from .sas7bdat datasets

% csvfile = 'C:\TAQ\taq2crsp\data\CRSPmsenames.csv';

% Import
tmp = dataset('File',csvfile,'Delimiter',',');

% Remove duplicates
tmp = unique(tmp);

% Drop the LIBRARY_Id 
tmp = tmp(:, setdiff(tmp.Properties.VarNames, 'LIBRARY_Id','stable'));

% Remove 'x___' as in x___PERMNO
tmp.Properties.VarNames = regexprep(tmp.Properties.VarNames,'x___','');

% Convert 19600101-based serial dates to yyyymmdd format
fmt = 'yyyymmdd';
c   = datenum('19600101',fmt);
f   = @(var) strcat('"',cellstr(datestr(cellfun(@(x) sscanf(x,'"%f"') + c, var),fmt)),'"');
tmp = replacedata(tmp, f, 'NAMEDT');
tmp = replacedata(tmp, f, 'NAMEENDT');

% Replace missing with \N, for MySQL's LOAD INFILE
for v = tmp.Properties.VarNames
     idx = strcmp(tmp.(v{:}),'""');
     tmp.(v{:})(idx) = {'"\N"'};
end

% Export
export(tmp,'file', strrep(csvfile,'.csv','_mod.csv') ,'Delimiter',',');

end
