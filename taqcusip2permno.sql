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
  `naics` mediumint(7) unsigned DEFAULT NULL,
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
INTO TABLE hfbetas.crsp_msenames character set utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES
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
  
#---------------------------------------------------------------------------------------------------
# MAP CRSP TO TAQ
#---------------------------------------------------------------------------------------------------

# 1) ON CUSIPS
#--------------

UPDATE final ff,  (select distinct q.permno, f.cusip
						from final f join crsp_stocknames q on f.cusip = q.ncusip) qq
SET ff.permno = qq.permno, ff.score = 10
WHERE ff.cusip = qq.cusip;

# 2) ON SYMBOL
#-------------

# SCORE: 20; Match D) SYMBOL = TICKER + DATEF within min/max of date ranges (name or data)
UPDATE final ff,  
	(select distinct q.permno, f.cusip, f.datef, f.name, f.symbol
		from (select * from final where permno is null) f 
			join crsp_stocknames q 
			on f.symbol = q.ticker AND (f.datef BETWEEN least(q.namedt, q.st_date) and greatest(q.end_date,q.nameenddt))
	) qq
SET ff.permno = qq.permno, ff.score = 20
WHERE ff.symbol = qq.symbol AND ff.datef = qq.datef;

# SCORE: 21; propagate permno for backed-out cusips (from matched symbol) also on non-matched date ranges
UPDATE final f,  
	(select distinct cusip, permno from final where score = 20) q 
SET f.permno = q.permno, f.score = 21
WHERE f.cusip = q.cusip AND f.permno is null;

select score, count(*), count(score)*100/count(*)
	from final
	group by score with rollup;