load debugstate
% NSCC field values:
%   NYSE 0, NYSE at issue 100
%   MKT  1, MKT  at issue 101
%   NASD 2, NASD at issue 102
%% SYMBOL checks NASD

% Symbol lengths
symblen = cellfun('size',TAQmaster.SYMBOL,2);
% Index NASD issues
iNASD = ismember(TAQmaster.NSCC, [2, 102]);
% [NO] Check if all 4-5 lengths
unique(symblen(iNASD))
% [NO] Does NASD means ETT or ETO?
TAQmaster(~(TAQmaster.ETT | TAQmaster.ETO) & iNASD,:)
unique(TAQmaster.NSCC(symblen == 4 | symblen == 5))

% SYMBOLS by CUSIP
[unCUSIP8,~,subs] = unique(TAQmaster.CUSIP8);
tmp = accumarray(subs, (1:size(subs,1))', [], @(x) {unique(TAQmaster.SYMBOL(x))});
idx = cellfun('prodofsize',tmp) > 1;

% [NO] Check if CUSIP9 distinguishes between same CUSIP different SYMBOL
[unCUSIP8,~,subs] = unique(TAQmaster.CUSIP8);
tmp               = accumarray(subs, char(TAQmaster.CUSIP9),[],@(x) {unique(x)});
unCUSIP8(cellfun('prodofsize',tmp)>1,:)

% [NO] Check if NSCC distinguishes between same CUSIP different SYMBOL
[unNSCC,~,map] = unique(TAQmaster.NSCC);
iNYSE          = accumarray(subs, map,[],@(x) any(x == 2 | x == 5));
iMKT           = accumarray(subs, map,[],@(x) any(x == 3 | x == 6));
iNASD          = accumarray(subs, map,[],@(x) any(x == 4 | x == 7));
idx            = (iNYSE & iMKT) | (iNYSE & iNASD) | (iMKT & iNASD);

unCUSIP8(idx)
sortrows(TAQmaster(strcmpi(TAQmaster.CUSIP8,{'00036020'}),:),'FDATE')

% [Do I care?] Drop CUSIP '233092102001', since it's the only CUSIP8 with two different CUSIP9!
% Actually I expected the issue same cusip multiple symbols to be signalled
% by multiple issues, but that's not the case. Maybe I should just go with
% back join to final after cusip-date join on crsp. 
TAQmaster(strcmpi(TAQmaster.CUSIP,'233092102001'),:) = [];