## TAQ & CRSP cusip merge scripts ##

# Prepare tables for import CRSP
SET GLOBAL innodb_file_per_table=1;
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

## Load CRSP stocknames
LOAD DATA INFILE '..\\..\\taq2crsp\\data\\CRSPstocknames.csv'
INTO TABLE hfbetas.crsp_stocknames character set utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES
(permno,permco,namedt,nameenddt,cusip,ncusip,ticker,comnam,hexcd,exchcd,siccd,shrcd,shrcls,st_date,end_date,namedum);

## Load TAQ master files
# full data
# LOAD DATA INFILE '..\\..\\taq2crsp\\data\\TAQmasterfiles.csv'
# INTO TABLE hfbetas.taq_master character set utf8 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES
# (symbol,name,cusip,etxn,etxa,etxb,etxp,etxx,etxt,etxo,etxw,its,icode,denom,type,datef);

# barebone master files
LOAD DATA INFILE '..\\..\\taq2crsp\\data\\TAQsymbols.csv'
INTO TABLE hfbetas.taqcusips character set utf8 FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES
(cusip,symbol,name,datef);

## Load TAQ code and type
LOAD DATA INFILE '..\\..\\taq2crsp\\data\\TAQcodetype.csv'
INTO TABLE hfbetas.taqcodetype character set utf8 FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES
(cusip,symbol,datef,icode,type);


## STEP1) Consolidate CRSP stocknames into essential info, to avoid join duplications later
create table crsp (PK int not null auto_increment, permno int, ncusip char(8), namedt int,
                    nameenddt int, ticker varchar(10), comnam varchar(40),
                    primary key (PK) KEY_BLOCK_SIZE=8,
				    KEY `crsp_ncusip` 	 (`cusip`),
					KEY `crsp_namedt` 	 (`namedt`),
					KEY `crsp_nameenddt` (`nameenddt`),
					KEY `crsp_ticker` 	 (`ticker`),
					KEY `crsp_comnam` 	 (`comnam`))
engine=InnoDB;
# copy all taqcusips to final
INSERT INTO final (`cusip`, `datef`, `symbol`, `name`)
select distinct taq.cusip, taq.datef, taq.symbol, taq.name 
	from taqcusips taq;

# Create final table & copy all entries from TAQcusips
create table final (PK int not null auto_increment, ID int, permno int, cusip char(8), symbol varchar(10), 
					name varchar(30), datef int, 
                    primary key (PK) KEY_BLOCK_SIZE=8,
				    KEY `final_ID` (`ID`),
					KEY `final_cusip` (`cusip`),
					KEY `final_datef` (`datef`),
					KEY `final_symbol` (`symbol`),
					KEY `final_name` (`name`))				
engine=InnoDB;
# copy all taqcusips to final
INSERT INTO final (`cusip`, `datef`, `symbol`, `name`)
select distinct taq.cusip, taq.datef, taq.symbol, taq.name 
	from taqcusips taq;
  

# Check uniqueness of entries in final 
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

# Redundancy check 
select *
	from taqcusips
	group by cusip, datef, symbol, name
	having count(*) > 1;

# STEP 2) merge with crsp data

# a) on cusips
# Counts of simple cusip to ncusip. 
select count(*)
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
union
select count(*)
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
			AND (f.datef BETWEEN q.namedt and q.nameenddt);

# Big difference, but only 2232 records are not matched by the more stringent condition with dates.
# In fact, the difference in counts comes mostly from duplication in the simple cusip = ncusip. Basically,
# the fdate crossjoins each namedate range.
select count(distinct f.pk)
	from final f join crsp_stocknames q on f.cusip = q.ncusip;

# TAQ fdate might be outside the [namedt - nameenddt] range. In this case we lose the match. 
# Also, cannot use [st_date, end_date] because it might be a smaller range than the namedate one.
select count(distinct l.pk)
	# Simple cusip = ncusip join ...
	from (select f.pk, f.permno, f.cusip, q.ncusip, q.namedt, f.datef, q.nameenddt, f.symbol, q.ticker, f.name
			from final f 
				join crsp_stocknames q 
				on f.cusip = q.ncusip) L 
		# ...against cusip=ncusip and date condition
		left join (select f.pk, f.cusip, q.ncusip, q.namedt, f.datef, q.nameenddt, f.symbol, f.name
					  from final f 
						 join crsp_stocknames q 
						 on f.cusip = q.ncusip AND f.datef BETWEEN q.namedt and q.nameenddt) R
		on L.PK = R.PK 
	where R.PK is null
	order by L.ncusip;

# Recover matches avoiding duplication by joining on fdate's month and year within namedate's month and year 
# Some sort of vintage date as in WRDS
select symbol
	from taqcusips
	group by symbol, cusip, month(datef), year(datef)
	having count(*) > 1;

# Check type of matches
# NOTE: rethink approach. We have same cusip with multiple SYMBOLS per date, traded on several exchanges. 
# We might want to do dateranges by CUSIP and SYMBOL.
# Example is:
select * 
	from taqcusips
	where cusip = '00081T10'
	order by datef;
	
select count(*) # cusip and date
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
			AND (f.datef BETWEEN q.namedt and q.nameenddt)
union
select count(*) # cusip and month
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
			AND (extract(year_month from f.datef) 
                 BETWEEN extract(year_month from q.namedt) and extract(year_month from q.nameenddt))
union
select count(*) # cusip, symbol and date
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip AND f.symbol = q.ticker
			AND (f.datef BETWEEN q.namedt and q.nameenddt)
union
select count(*) # cusip, symbol and month
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip AND f.symbol = q.ticker
			AND (extract(year_month from f.datef) 
                 BETWEEN extract(year_month from q.namedt) and extract(year_month from q.nameenddt));


# Check duplication in cusip and month
select count(*) from(
select f.pk, q.permno, f.cusip, q.ncusip, min(q.namedt) namedt, f.datef, max(q.nameenddt) nameenddt, f.symbol, q.ticker, f.name
	from final f 
		join crsp_stocknames q on f.cusip = q.ncusip
			AND (extract(year_month from f.datef) 
                 BETWEEN extract(year_month from q.namedt) and extract(year_month from q.nameenddt))
	#where f.pk = 4291
	group by q.permno, f.cusip, f.datef, q.ncusip ) F
	having count(*) > 1

select * from taqcusips where cusip in (select distinct cusip from taqcusips where symbol ='ABD');
select * from taqcusips where cusip in('00081T10');
select * from taqcusips where symbol ='AAG';

UPDATE final f, crsp_stocknames q
SET f.permno=q.permno, f.ncusip=q.ncusip, f.namedt=q.namedt, f.nameenddt=q.nameenddt, f.ticker=q.ticker
WHERE f.cusip=q.ncusip AND (f.datef BETWEEN q.namedt and q.nameenddt);

# b) on symbols
UPDATE final f,  crsp_stocknames q
SET f.permno=q.permno, f.ncusip=q.ncusip, f.namedt=q.namedt, f.nameenddt=q.nameenddt, f.ticker=q.ticker
WHERE f.ncusip IS NULL AND f.symbol=q.ticker AND (f.datef BETWEEN q.namedt and q.nameenddt);

select f.* 
		from final f 
			inner join crsp_stocknames q on f.symbol LIKE concat(q.ticker,'%') AND f.name LIKE concat(q.comnam,'%')
				AND (f.datef BETWEEN q.namedt and q.nameenddt)
		WHERE f.ncusip is null and q.ticker is not null and q.comnam is not null) q
SET f.permno=q.permno, f.ncusip=q.ncusip, f.namedt=q.namedt, f.nameenddt=q.nameenddt, f.ticker=q.ticker
where f.symbol = q.symbol and f.name = q.name and f.datef = q.datef;

/*
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


# Issue: same cusip and date, different symbols
# EXPLANATION: some symbols might e.g. be traded on a set of different exchanges
# When creating time-invariant ID, need to consider PERMNO with SYMBOL
select * 
	from (select distinct cusip, symbol, datef
			from taqcusips
			where cusip is not null) A
	group by cusip, datef
	having count(*) > 1
	order by cusip, datef;

## OLD:

/*
# TAQ master
CREATE TABLE `taq_master` (
`PK` int(10) unsigned NOT NULL AUTO_INCREMENT,
`symbol` varchar(10) DEFAULT NULL,
`name` varchar(30) DEFAULT NULL,
`cusip` varchar(8) DEFAULT NULL,
`etxn` tinyint(1) DEFAULT NULL,
`etxa` tinyint(1) DEFAULT NULL,
`etxb` tinyint(1) DEFAULT NULL,
`etxp` tinyint(1) DEFAULT NULL,
`etxx` tinyint(1) DEFAULT NULL,
`etxt` tinyint(1) DEFAULT NULL,
`etxo` tinyint(1) DEFAULT NULL,
`etxw` tinyint(1) DEFAULT NULL,
`its` tinyint(1) DEFAULT NULL,
`icode` char(4) DEFAULT NULL,
`denom` char(1) DEFAULT NULL,
`type` tinyint(3) unsigned DEFAULT NULL,
`datef` int(10) unsigned DEFAULT NULL,
PRIMARY KEY (`PK`) KEY_BLOCK_SIZE=8,
UNIQUE KEY `idtaq_master_UNIQUE` (`PK`) KEY_BLOCK_SIZE=8,
KEY `taq_master_cusip` (`cusip`),
KEY `taq_master_datef` (`datef`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
*/


/* # IGNORE FOR NOW
CREATE TABLE `crsp_permnotool` (
`PK` mediumint(8) unsigned NOT NULL AUTO_INCREMENT,
`date` int(10) unsigned DEFAULT NULL,
`comnam` varchar(40) DEFAULT NULL,
`ncusip` char(8) DEFAULT NULL,
`ticker` varchar(10) DEFAULT NULL,
`permno` int(10) unsigned DEFAULT NULL,
PRIMARY KEY `PRIMARY` (`PK`),
UNIQUE KEY `PK_UNIQUE` (`PK`)
) ENGINE=InnoDB;
# Load CRSP webtool permno
LOAD DATA LOCAL INFILE 'C:\\HFbetas\\data\\CRSP\\webtool_permno.csv'
INTO TABLE hfbetas.crsp_permnotool character set utf8 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES
(DATE, COMNAM, NCUSIP, TICKER, PERMNO);
*/


# CHECK - TAQ cusips that have a match in ncusip BUT not for all dates
# select R.*
# from
# (SELECT cusip
# FROM final
# group by cusip
# having sum(ncusip is null) < count(*) AND sum(ncusip is null) > 1) doubles
# inner join final R on doubles.cusip = R.cusip
# order by doubles.cusip;

# CHECK - counts [OK]
# --------------------------------------------------------
/*
SELECT count(*) tot, sum(case when isnull(cusip) then 1 else 0 end) nulls, sum(case when isnull(cusip) then 0 else 1 end) notnulls
FROM taqcusips;
select count(*) from final;
*/

# CHECK - back and forth of tickers in time for same cusip
# --------------------------------------------------------
/*
select distinct L.cusip, R.symbol, L.symbol, R.mindt, L.datef, R.maxdt
from taqcusips L
inner join
(select cusip, symbol, min(datef) mindt, max(datef) maxdt
from taqcusips
where cusip is not null
group by cusip, symbol) R
on L.cusip = R.cusip and L.symbol != R.symbol and L.datef >= R.mindt and L.datef < R.maxdt
order by L.cusip, R.mindt;

# Example
select * from final
where cusip = '00088E10'
order by datef;

select * from crsp_stocknames
where cusip = '00088E10'
order by namedt;
*/

# size of tables
SELECT TABLE_NAME, table_rows, data_length, index_length, round(((data_length + index_length) / 1024 / 1024),2) 'Size in MB'
FROM information_schema.TABLES
WHERE table_schema = 'hfbetas' and TABLE_TYPE='BASE TABLE'
ORDER BY data_length DESC;

#SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'hfbetas' AND TABLE_NAME = 'taq_master';