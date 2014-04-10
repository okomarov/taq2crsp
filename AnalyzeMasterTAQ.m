load debugstate
% ANALYZE How many ticker + letter in NASDAQ and MYSE and of which type

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