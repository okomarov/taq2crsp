## TAQ & CRSP cusip merge scripts ##

#---------------------------------------------------------------------------------------------------
# IMPORT DATA
#---------------------------------------------------------------------------------------------------
SET GLOBAL innodb_file_per_table=1;

# crsp_stocknames
CREATE TABLE `crsp_stocknames` (
  `PK` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `permno` int(10) DEFAULT NULL,
  `permco` int(11) DEFAULT NULL,
  `namedt` int(11) DEFAULT NULL,
  `nameenddt` int(11) DEFAULT NULL,
  `cusip` char(8) DEFAULT NULL,
  `ncusip` char(8) DEFAULT NULL,
  `ticker` varchar(10) DEFAULT NULL,
  `comnam` varchar(40) DEFAULT NULL,
  `hexcd` tinyint(4) DEFAULT NULL,
  `exchcd` tinyint(4) DEFAULT NULL,
  `siccd` smallint(5) unsigned DEFAULT NULL,
  `shrcd` tinyint(4) DEFAULT NULL,
  `shrcls` char(1) DEFAULT NULL,
  `st_date` int(10) unsigned DEFAULT NULL,
  `end_date` int(10) unsigned DEFAULT NULL,
  `namedum` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`PK`),
  UNIQUE KEY `PK_UNIQUE` (`PK`),
  KEY `stocknames_ncusip` (`ncusip`),
  KEY `stocknames_ticker` (`ticker`),
  KEY `stocknames_name` (`comnam`),
  KEY `stocknames_namedt` (`namedt`),
  KEY `stocknames_nameenddt` (`nameenddt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOAD DATA INFILE '..\\..\\taq2crsp\\data\\CRSPstocknames.csv'
INTO TABLE hfbetas.crsp_stocknames character set utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES
(permno,permco,namedt,nameenddt,cusip,ncusip,ticker,comnam,hexcd,exchcd,siccd,shrcd,shrcls,st_date,end_date,namedum);

# crsp_msenames
CREATE TABLE `crsp_msenames` (
  `PK` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `permno` int(10) DEFAULT NULL,
  `namedt` int(11) DEFAULT NULL,
  `nameenddt` int(11) DEFAULT NULL,
  `shrcd` tinyint(4) DEFAULT NULL,
  `exchcd` tinyint(4) DEFAULT NULL,
  `siccd` smallint(5) unsigned DEFAULT NULL,
  `ncusip` char(8) DEFAULT NULL,
  `ticker` varchar(10) DEFAULT NULL,
  `comnam` varchar(40) DEFAULT NULL,
  `shrcls` char(1) DEFAULT NULL,
  `tsymbol` varchar(10) DEFAULT NULL,
  `naics` mediumint unsigned DEFAULT NULL,
  `primexch` char(1) NOT NULL, 
  `trdstat` char(1) NOT NULL, 
  `secstat` char(1) NOT NULL, 
  `permco` int(11) DEFAULT NULL,
  `compno` int(8) unsigned, 
  `issuno` int(8) unsigned, 
  `hexcd` tinyint(4) DEFAULT NULL,
  `hsiccd` smallint(5) unsigned DEFAULT NULL,
  `cusip` char(8) DEFAULT NULL,
  PRIMARY KEY (`PK`),
  UNIQUE KEY `PK_UNIQUE` (`PK`),
  KEY `stocknames_ncusip` (`ncusip`),
  KEY `stocknames_ticker` (`ticker`),
  KEY `stocknames_name` (`comnam`),
  KEY `stocknames_namedt` (`namedt`),
  KEY `stocknames_nameenddt` (`nameenddt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOAD DATA INFILE '..\\..\\taq2crsp\\data\\CRSPmsenames.csv'
INTO TABLE hfbetas.crsp_msenames character set utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES
(permno,namedt,nameenddt,shrcd,exchcd,siccd,ncusip,ticker,comnam,shrcls,tsymbol,naics,primexch,trdstat,secstat,permco,compno,issuno,hexcd,hsiccd,cusip);

# TAQ symbols only
SET GLOBAL innodb_file_per_table=1;
CREATE TABLE `taqcusips` (
  `PK` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `cusip` varchar(8) DEFAULT NULL,
  `symbol` varchar(10) DEFAULT NULL,
  `name` varchar(40) DEFAULT NULL,
  `datef` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`PK`) KEY_BLOCK_SIZE=8,
  UNIQUE KEY `taqcusips_UNIQUE` (`PK`) KEY_BLOCK_SIZE=8,
  KEY `taqcusips_cusip` (`cusip`),
  KEY `taqcusips_datef` (`datef`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOAD DATA INFILE '..\\..\\taq2crsp\\data\\TAQsymbols.tab'
INTO TABLE hfbetas.taqcusips character set utf8 FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES
(cusip,symbol,name,datef);

# TAQ code and type
SET GLOBAL innodb_file_per_table=1;
CREATE TABLE `taqcodetype` (
  `PK` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `cusip` varchar(8) DEFAULT NULL,
  `symbol` varchar(10) DEFAULT NULL,
  `datef` int(10) unsigned DEFAULT NULL,
  `icode` varchar(4) DEFAULT NULL,
  `type` tinyint(1) unsigned DEFAULT NULL,
  PRIMARY KEY (`PK`) KEY_BLOCK_SIZE=8,
  UNIQUE KEY `taqcodetype_UNIQUE` (`PK`) KEY_BLOCK_SIZE=8,
  KEY `taqcodetype_cusip` (`cusip`),
  KEY `taqcodetype_datef` (`datef`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOAD DATA INFILE '..\\..\\taq2crsp\\data\\TAQcodetype.tab'
INTO TABLE hfbetas.taqcodetype character set utf8 FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES
(cusip,symbol,datef,icode,type);

#---------------------------------------------------------------------------------------------------
# POST-IMPORT PROCESSING
#---------------------------------------------------------------------------------------------------

# Create final table & copy all entries from TAQcusips, i.e. it's the target set 
create table final (PK int not null auto_increment, ID int, permno int, cusip char(8), symbol varchar(10), 
					name varchar(30), datef int, score tinyint, 
                    primary key (PK) KEY_BLOCK_SIZE=8,
				    KEY `final_ID` (`ID`),
					KEY `final_cusip` (`cusip`),
					KEY `final_datef` (`datef`),
					KEY `final_symbol` (`symbol`),
					KEY `final_name` (`name`))				
engine=InnoDB;

INSERT INTO final (`cusip`, `datef`, `symbol`, `name`)
select distinct taq.cusip, taq.datef, taq.symbol, taq.name 
	from taqcusips taq;
  
# Check uniqueness of entries in final [should give one count]
select count(*) # distinct count
	from (select distinct cusip, datef, symbol, name 
			from taqcusips) A
union # group by count
select count(*)
	from (select * 
			from taqcusips
			group by cusip, datef, symbol, name) A
union # simple count
select count(*)
	from taqcusips;

# Redundancy check [should be empty]
select *
	from taqcusips
	group by cusip, datef, symbol, name
	having count(*) > 1;

#---------------------------------------------------------------------------------------------------
# MAP CRSP TO TAQ
#---------------------------------------------------------------------------------------------------

# 1) ON CUSIPS
#--------------

# CUSIP join counts for tclink comparison (they don't impose datef condition)

# Since TAQ fdate might be < namedt by a few days and I would lose the match, I also try
# the [st_date, end_date], but cannot use it alone because st_date can be > namedt (see C) below).
# Therefore, I try the min of the two ranges.
# NOTE: I count distinct primary key rows, since in the simple cusip = ncusip join 
#       the fdate crossjoins each namedate range (even for entries out of range, but same cusip).

select 'A) cusip = ncusip' description, count(distinct f.pk) counts, count(distinct f.pk)/max(f.pk)*100 Perc # has crossjoin
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
union
select 'B) A + datef in [namedt, nameenddt]', count(*), count(*)/max(f.pk)*100 # no crossjoin
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
			AND (f.datef BETWEEN q.namedt and q.nameenddt)
union
select 'C) A + datef in [st_date, end_date]', count(distinct f.pk), count(distinct f.pk)/max(f.pk)*100 # has crossjoin
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
			AND (f.datef BETWEEN q.st_date and q.end_date)
union
select 'D) A + min/max of date ranges', count(distinct f.pk), count(distinct f.pk)/max(f.pk)*100  # has crossjoin
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
			AND (f.datef BETWEEN least(q.namedt, q.st_date) and greatest(q.end_date,q.nameenddt))
union
select 'E) B with yy/mm dates',count(distinct f.pk), count(distinct f.pk)/max(f.pk)*100  # has crossjoins
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
			AND (extract(year_month from f.datef) 
				BETWEEN extract(year_month from q.namedt) and extract(year_month from q.nameenddt))
union
select 'F) B + symbol = ticker', count(*),count(*)/max(f.pk)*100  # no crossjoin
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip AND f.symbol = q.ticker
			AND (f.datef BETWEEN q.namedt and q.nameenddt)
union
select 'G) F with yy/mm dates', count(distinct f.pk), count(distinct f.pk)/max(f.pk)*100  # has crossjoin
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip AND f.symbol = q.ticker
			AND (extract(year_month from f.datef) 
                 BETWEEN extract(year_month from q.namedt) and extract(year_month from q.nameenddt));

# Course of action:
# - [skipping] Check which cases have st_date < namedt (by permno?) or end_date > nameenddt and then decide if to widen the date range.
# - Leave the symbol out, since the join should be a proper subset of B and I can't think on ways it could improve on B.

# Setdiff of A) vs B) for inspection and count check - A matches all B
#select count(distinct l.pk)
select L.*
	# cusip = ncusip join and min/max of date ranges ...
	from (select f.pk, f.permno, f.cusip, q.ncusip, q.namedt, f.datef, q.nameenddt, f.symbol, q.ticker, f.name
			FROM final f JOIN crsp_stocknames q ON f.cusip = q.ncusip) L 
		# ...against cusip=ncusip and date condition
		left join
		# right join
			(select f.pk, f.cusip, q.ncusip, q.namedt, f.datef, q.nameenddt, f.symbol, f.name
				FROM final f JOIN crsp_stocknames q ON f.cusip = q.ncusip AND f.datef BETWEEN q.namedt and q.nameenddt) R
		on L.PK = R.PK 
	where R.PK is null
	#where L.PK is null
	order by L.ncusip;

# Setdiff of D) vs B) for inspection and count check - D matches all B
#select count(distinct l.pk)
select L.*
	# Simple cusip = ncusip join ...
	from (select f.pk, f.permno, f.cusip, q.ncusip, q.namedt, f.datef, q.nameenddt, f.symbol, q.ticker, f.name
			FROM final f JOIN crsp_stocknames q ON f.cusip = q.ncusip AND (f.datef BETWEEN least(q.namedt, q.st_date) and greatest(q.end_date,q.nameenddt))) L 
		# ...against cusip=ncusip and date condition
		left join
		# right join 
			(select f.pk, f.cusip, q.ncusip, q.namedt, f.datef, q.nameenddt, f.symbol, f.name
				FROM final f JOIN crsp_stocknames q ON f.cusip = q.ncusip AND f.datef BETWEEN q.namedt and q.nameenddt) R
		on L.PK = R.PK 
	where R.PK is null
	#where L.PK is null
	order by L.ncusip;

set @cusip = '02263A10';
select *, '' from taqcusips where cusip = @cusip
union
select distinct PK, ncusip, ticker, comnam, namedt, nameenddt from crsp_stocknames where ncusip = @cusip;
select * from crsp_stocknames where ncusip = @cusip;
select distinct PK, ncusip, ticker, comnam, namedt, nameenddt from crsp_stocknames where permno = 10401;

# SCORE: 10; Match D) CUSIP = NCUSIP + DATEF within min/max of date ranges (name or data)
UPDATE final ff,  
	(select distinct q.permno, f.cusip, f.datef, f.symbol
		from final f 
			join crsp_stocknames q 
			on f.cusip = q.ncusip AND (f.datef BETWEEN least(q.namedt, q.st_date) and greatest(q.end_date,q.nameenddt))
	) qq
SET ff.permno = qq.permno, ff.score = 10
WHERE ff.cusip = qq.cusip AND ff.datef = qq.datef;

# Cusips that have a match but not on all date ranges (count 774 cusips, 870 records)
#select count(distinct f.pk) #count(distinct f.cusip)  
select f.*
	from final f 
		join (select cusip
				from final
				group by cusip
				having  sum(isnull(permno)) > 0 and sum(not isnull(permno)) > 0) q 
		on f.cusip = q.cusip
	#where score is null
	order by f.cusip, f.datef;

# SCORE: 11; propagate permno for matched cusips also on non-matched date ranges
UPDATE final f,  
	(select distinct cusip, permno from final where score = 10) q 
SET f.permno = q.permno, f.score = 11
WHERE f.cusip = q.cusip AND f.permno is null;

# Check how D) + propagation is different from E), i.e. the yy/mm datef match to unmatched, 
# [doing d), propagation and then only mm/yy date match, so that matches belonging to intra-month datef < nameenddt don't expand also to next range]

# 2) ON SYMBOL
#-------------

# Simple symbol to ticker match might be incorrect because different companies might 
# re-use the ticker at different points in time. 
# Matching the residual entries (no permno).

select 'A) symbol = ticker' description, count(distinct f.pk) counts, count(distinct f.pk)/max(f.pk)*100 Perc # has crossjoin
	from (select * from final where permno is null) f 
		join crsp_stocknames q on f.symbol = q.ticker
union
select 'B) A + datef in [namedt, nameenddt]', count(*), count(*)/max(f.pk)*100 # no crossjoin
	from (select * from final where permno is null) f 
		join crsp_stocknames q on f.symbol = q.ticker
			AND (f.datef BETWEEN q.namedt and q.nameenddt)
union
select 'C) A + datef in [st_date, end_date]', count(distinct f.pk), count(distinct f.pk)/max(f.pk)*100 # has crossjoin
	from (select * from final where permno is null) f 
		join crsp_stocknames q on f.symbol = q.ticker
			AND (f.datef BETWEEN q.st_date and q.end_date)
union
select 'D) A + min/max of date ranges', count(distinct f.pk), count(distinct f.pk)/max(f.pk)*100  # has crossjoin
	from (select * from final where permno is null) f 
		join crsp_stocknames q on f.symbol = q.ticker
			AND (f.datef BETWEEN least(q.namedt, q.st_date) and greatest(q.end_date,q.nameenddt))
union
select 'E) B with yy/mm dates',count(distinct f.pk), count(distinct f.pk)/max(f.pk)*100  # has crossjoins
	from (select * from final where permno is null) f 
		join crsp_stocknames q on f.symbol = q.ticker
			AND (extract(year_month from f.datef) 
				BETWEEN extract(year_month from q.namedt) and extract(year_month from q.nameenddt));

# SCORE: 20; Match D) SYMBOL = TICKER + DATEF within min/max of date ranges (name or data)
UPDATE final ff,  
	(select distinct q.permno, f.cusip, f.datef, f.name, f.symbol
		from (select * from final where permno is null) f 
			join crsp_stocknames q 
			on f.symbol = q.ticker AND (f.datef BETWEEN least(q.namedt, q.st_date) and greatest(q.end_date,q.nameenddt))
	) qq
SET ff.permno = qq.permno, ff.score = 20
WHERE ff.symbol = qq.symbol AND ff.datef = qq.datef;

# Cannot propagate by symbol because it can be re-used by a different company.
# Work out the cusip for the matched symbol and propagate 
#select count(distinct L.pk)
select *
	from final L 
		join (select distinct cusip 
				from final 
				group by cusip 
				having sum(isnull(permno)) > 0 and sum(not isnull(permno)) > 0) R
		on L.cusip = R.cusip
	# where permno is null
	order by L.cusip, L.datef;

# SCORE: 21; propagate permno for backed-out cusips (from matched symbol) also on non-matched date ranges
UPDATE final f,  
	(select distinct cusip, permno from final where score = 20) q 
SET f.permno = q.permno, f.score = 21
WHERE f.cusip = q.cusip AND f.permno is null;

select score, count(*), count(score)*100/count(*)
	from final
	group by score with rollup;

#---------------------------------------------------------------------------------------------------
# COMPARE AGAINST NO DATE CUSIP MATCH - BETTER!
#---------------------------------------------------------------------------------------------------
# Create final table & copy all entries from TAQcusips, i.e. it's the target set 
create table final4 (PK int not null auto_increment, ID int, permno int, cusip char(8), symbol varchar(10), 
					name varchar(30), datef int, score tinyint, 
                    primary key (PK) KEY_BLOCK_SIZE=8,
				    KEY `final_ID` (`ID`),
					KEY `final_cusip` (`cusip`),
					KEY `final_datef` (`datef`),
					KEY `final_symbol` (`symbol`),
					KEY `final_name` (`name`),
					KEY `final_permno` (`permno`))				
engine=InnoDB;

INSERT INTO final4 (`cusip`, `datef`, `symbol`, `name`)
select distinct taq.cusip, taq.datef, taq.symbol, taq.name 
	from taqcusips taq;

# CUSIP
UPDATE final4 ff,  (select distinct q.permno, f.cusip
					from final4 f 
						join crsp_stocknames q 
						on f.cusip = q.ncusip) qq
SET ff.permno = qq.permno, ff.score = 10
WHERE ff.cusip = qq.cusip;

# Check against names and symbols (166 potential wrong matches)
# In reality a levenshtein distance would be more appropriate, i.e. I expect only 10% of
# the wrong matches to be true
select ff.*
	from (select f.*
			from final4 f
				left join (select distinct permno, ticker, comnam from crsp_stocknames) q 
				on f.permno = q.permno and f.symbol like concat(q.ticker,'%')
			where q.permno is null and score is not null) ff 
		left join (select distinct permno, ticker, comnam from crsp_stocknames) qq
		on ff.permno = qq.permno and ff.name like concat(qq.comnam,'%')
	where qq.permno is null;

set @permno = 80311;
select distinct ncusip, ticker, comnam, namedt,nameenddt,1 a
	from crsp_stocknames
	where permno  = @permno
union
select distinct cusip, symbol, name, datef, 2,2
	from final4
	where permno  = @permno
	order by a, namedt;

# Compare against final (cusip match only)
select * 
	from final4 f4 
		join final f on f4.pk = f.pk
	where f4.permno <> f.permno;

select * # 961 more matches
	from final4 f4 
		join final f on f4.pk = f.pk
	where f4.permno is not null and f.permno is null;

select * # obviously 0
	from final4 f4 
		join final f on f4.pk = f.pk
	where f4.permno is  null and f.permno is not null;	

#---------------------------------------------------------------------------------------------------
# COMPARE AGAINST TCLINK
#---------------------------------------------------------------------------------------------------

# wrds_tclink
CREATE TABLE `wrds_tclink` (
  `PK` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `permno` int(10) DEFAULT NULL,
  `cusip` char(8) DEFAULT NULL,  
  `date` int(11) unsigned DEFAULT NULL,
  `symbol` varchar(10) DEFAULT NULL,
  `score` tinyint(1) unsigned DEFAULT 0,
  PRIMARY KEY (`PK`),
  UNIQUE KEY `PK_UNIQUE` (`PK`),
  KEY `wrdstclink_cusip` (`cusip`),
  KEY `wrdstclink_symbol` (`symbol`),
  KEY `wrdstclink_date` (`date`),
  KEY `wrdstclink_permno` (`permno`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOAD DATA INFILE '..\\..\\taq2crsp\\data\\WRDStclink.csv'
INTO TABLE hfbetas.wrds_tclink character set utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES
(permno,cusip,date,symbol,score);


#---------------------------------------------------------------------------------------------------
# COMPARE AGAINST MM/YY MATCHES, i.e. E)
#---------------------------------------------------------------------------------------------------

# Looks like vintage date avoids some erroneous matches, but need to check why fails in a specific case below

# Create final table & copy all entries from TAQcusips, i.e. it's the target set 
create table final3 (PK int not null auto_increment, ID int, permno int, cusip char(8), symbol varchar(10), 
					name varchar(30), datef int, score tinyint, 
                    primary key (PK) KEY_BLOCK_SIZE=8,
				    KEY `final_ID` (`ID`),
					KEY `final_cusip` (`cusip`),
					KEY `final_datef` (`datef`),
					KEY `final_symbol` (`symbol`),
					KEY `final_name` (`name`))				
engine=InnoDB;

INSERT INTO final3 (`cusip`, `datef`, `symbol`, `name`)
select distinct taq.cusip, taq.datef, taq.symbol, taq.name 
	from taqcusips taq;

# CUSIP
UPDATE final3 ff,  
	(select distinct q.permno, f.cusip, f.datef, f.name, f.symbol
		from final3 f 
			join crsp_stocknames q 
			on f.cusip = q.ncusip AND 
				(extract(year_month from f.datef) BETWEEN extract(year_month from q.namedt) and extract(year_month from q.nameenddt))
	) qq
SET ff.permno = qq.permno, ff.score = 10
WHERE ff.cusip = qq.cusip AND ff.datef = qq.datef AND ff.symbol = qq.symbol;
# Propagate
UPDATE final3 f,  
	(select distinct cusip, permno from final3 where score = 10) q 
SET f.permno = q.permno, f.score = 11
WHERE f.cusip = q.cusip AND f.permno is null;

# SYMBOL
UPDATE final3 ff,  
	(select distinct q.permno, f.cusip, f.datef, f.name, f.symbol
		from (select * from final3 where permno is null) f 
			join crsp_stocknames q 
			on f.symbol = q.ticker AND (extract(year_month from f.datef) BETWEEN extract(year_month from q.namedt) and extract(year_month from q.nameenddt))
	) qq
SET ff.permno = qq.permno, ff.score = 20
WHERE ff.symbol = qq.symbol AND ff.datef = qq.datef;
# Propagate
UPDATE final3 f,  
	(select distinct cusip, permno from final3 where score = 20) q 
SET f.permno = q.permno, f.score = 21
WHERE f.cusip = q.cusip AND f.permno is null;


# Take difference 
select * 
	from final3 f3 
		join final f on f3.pk = f.pk
	where f3.permno <> f.permno;

select *
	from final3 f3 
		join final f on f3.pk = f.pk
	where f3.permno is not null and f.permno is null;

select *
	from final3 f3 
		join final f on f3.pk = f.pk
	where f3.permno is  null and f.permno is not null;	

select * from crsp_stocknames where permno = 80170;

# Check this match: IFS change of company, until october it's some permno AND from september is also a different one!
select * 
	from crsp_stocknames
	where permno in (79701, 86345);

select score, count(*), count(score)*100/count(*)
	from final3
	group by score with rollup;

#---------------------------------------------------------------------------------------------------
# COMPARE AGAINST INTIAL APPROACH
#---------------------------------------------------------------------------------------------------

# STEP 1) create final table & copy all entries from TAQcusips
create table final2 (PK int not null auto_increment, permno int, cusip char(8), ncusip char(8), namedt int,
                    datef int, nameenddt int, symbol varchar(10), ticker varchar(10), name varchar(30),
                    primary key (PK) KEY_BLOCK_SIZE=8,
				    KEY `final_cusip` (`cusip`),
					KEY `final_datef` (`datef`),
					KEY `final_symbol` (`symbol`),
					KEY `final_name` (`name`))
					
engine=InnoDB;

# copy all taqcusips to final
INSERT INTO final2 (`cusip`, `datef`, `symbol`, `name`)
select taq.cusip, taq.datef, taq.symbol, taq.name 
	from taqcusips taq;
  
# STEP 2) merge with crsp data
# a) on cusips
UPDATE final2 f, crsp_stocknames q
SET f.permno=q.permno, f.ncusip=q.ncusip, f.namedt=q.namedt, f.nameenddt=q.nameenddt, f.ticker=q.ticker
WHERE f.cusip=q.ncusip AND (f.datef BETWEEN q.namedt and q.nameenddt);

# b) on symbols
UPDATE final2 f,  crsp_stocknames q
SET f.permno=q.permno, f.ncusip=q.ncusip, f.namedt=q.namedt, f.nameenddt=q.nameenddt, f.ticker=q.ticker
WHERE f.ncusip IS NULL AND f.symbol=q.ticker AND (f.datef BETWEEN q.namedt and q.nameenddt);

/*
select f.* 
		from final f 
			inner join crsp_stocknames q on f.symbol LIKE concat(q.ticker,'%') AND f.name LIKE concat(q.comnam,'%')
				AND (f.datef BETWEEN q.namedt and q.nameenddt)
		WHERE f.ncusip is null and q.ticker is not null and q.comnam is not null) q
SET f.permno=q.permno, f.ncusip=q.ncusip, f.namedt=q.namedt, f.nameenddt=q.nameenddt, f.ticker=q.ticker
where f.symbol = q.symbol and f.name = q.name and f.datef = q.datef;

# c) on similar cymbols and company
explain UPDATE final f, crsp_stocknames q
SET f.permno=q.permno, f.ncusip=q.ncusip, f.namedt=q.namedt, f.nameenddt=q.nameenddt, f.ticker=q.ticker
WHERE f.ncusip is null AND f.symbol LIKE concat(q.ticker,'%') AND f.name LIKE concat(q.comnam,'%')
AND (f.datef BETWEEN q.namedt and q.nameenddt);

explain UPDATE final f, 
	(select f.*, q.*
		from final f 
			inner join crsp_stocknames q on f.symbol LIKE concat(q.ticker,'%') AND f.name LIKE concat(q.comnam,'%')
				AND (f.datef BETWEEN q.namedt and q.nameenddt)
		WHERE f.ncusip is null and q.ticker is not null and q.comnam is not null) q
SET f.permno=q.permno, f.ncusip=q.ncusip, f.namedt=q.namedt, f.nameenddt=q.nameenddt, f.ticker=q.ticker
where f.symbol = q.symbol and f.name = q.name and f.datef = q.datef;

select f.* 
	from final f 
		inner join crsp_stocknames q on f.symbol LIKE concat(q.ticker,'%') AND f.name LIKE concat(q.comnam,'%')
				AND (f.datef BETWEEN q.namedt and q.nameenddt)
	WHERE f.ncusip is null and q.ticker is not null and q.comnam is not null
*/

#---------------------------------------------------------------------------------------------------
# EXAMPLES OF ISSUES
#---------------------------------------------------------------------------------------------------

# PROBLEM: We have same cusip with multiple SYMBOLS per date. 
select * from taqcusips where cusip = '00081T10' order by datef;
# PROBLEM: wrong cusip in TAQ?
select *, '' from taqcusips where cusip in  ('00123020','00123010')
union
select distinct PK, ncusip, ticker, comnam, namedt, nameenddt from crsp_stocknames where ncusip in  ('00123020','00123010');
# PROBLEM: match doesn't propagate (would need to check how long prices go) - support widening range to min max
set @cusip = '00141J10';
select *, '' from taqcusips where cusip = @cusip
union
select distinct PK, ncusip, ticker, comnam, namedt, nameenddt from crsp_stocknames where ncusip = @cusip;
select * from crsp_stocknames where ncusip = @cusip;

# Check duplication in cusip and month
select count(*) from(
select f.pk, q.permno, f.cusip, q.ncusip, min(q.namedt) namedt, f.datef, max(q.nameenddt) nameenddt, f.symbol, q.ticker, f.name
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
			AND (extract(year_month from f.datef) 
                 BETWEEN extract(year_month from q.namedt) and extract(year_month from q.nameenddt))
	#where f.pk = 4291
	group by q.permno, f.cusip, f.datef, q.ncusip ) F
	having count(*) > 1;

select * from taqcusips where cusip in (select distinct cusip from taqcusips where symbol ='ABD');
select * from taqcusips where cusip in('00081T10');
select * from taqcusips where symbol ='AAG';


# Q: same name different permnos?
select * 
	from crsp_msenames L 
		join (select comnam
				from crsp_msenames
				group by comnam
				having count(distinct permno) > 1) R on L.comnam = R.comnam
	order by L.comnam, namedt
# A: yes, even on same time frames, usually different SHRCLS (share class)


# size of tables
SELECT TABLE_NAME, table_rows, data_length, index_length, round(((data_length + index_length) / 1024 / 1024),2) 'Size in MB'
FROM information_schema.TABLES
WHERE table_schema = 'hfbetas' and TABLE_TYPE='BASE TABLE'
ORDER BY data_length DESC;