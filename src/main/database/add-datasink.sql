-- Run the following statements to make System Builder UI tool aware of the datasink.
INSERT INTO APPLICATION.PROCESS_GROUP_CONFIG VALUES(
	'datasink.cvquery', 
	'datasink.default', 
	'private', 
	'cv-query?.@build.domain@', 
	'ingest.rtws.saic.com', 
	null, 
	'm1.large', 
	'instance', 
	null, 
	'datasink-cvquery.ini', 
	'services.cvquery.xml', 
	'{"default-num-volumes" : 0, "default-volume-size" : 0, "config-volume-size" : false, "config-persistent-ip" : false, "default-num-instances" : 1, "config-instance-size" : true, "config-min-max" : true, "config-scaling" : true, "config-jms-persistence" : false }'
);
	
INSERT INTO APPLICATION.DATASINK_CONFIG VALUES(
	'gov.usdot.cv.query.datasink.QueryProcessor',
	'Y',
	'N',
	0.75,
	'',
	'',
	'',
	'datasink.cvquery'
);	