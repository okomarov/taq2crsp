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

# STEP 1) create final table & copy all entries from TAQcusips
create table final (PK int not null auto_increment, permno int, cusip char(8), ncusip char(8), namedt int,
                    datef int, nameenddt int, symbol varchar(10), ticker varchar(10), name varchar(30),
                    primary key (PK) KEY_BLOCK_SIZE=8,
				    KEY `final_cusip` (`cusip`),
					KEY `final_datef` (`datef`),
					KEY `final_symbol` (`symbol`),
					KEY `final_name` (`name`))
					
engine=InnoDB;

# copy all taqcusips to final
INSERT INTO final (`cusip`, `datef`, `symbol`, `name`)
select taq.cusip, taq.datef, taq.symbol, taq.name 
	from taqcusips taq;
  
# STEP 2) merge with crsp data
# a) on cusips
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