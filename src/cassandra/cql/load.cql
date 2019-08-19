CREATE KEYSPACE IF NOT EXISTS demo_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS demo_keyspace.parent_entity (
	partitionKey text,
	clusterKey text,
	text text,
	PRIMARY KEY (partitionKey, clusterKey)) WITH CLUSTERING ORDER BY (clusterKey ASC);
	
CREATE TABLE IF NOT EXISTS demo_keyspace.child_entity (
	partitionKey text,
	clusterKey text,
	text text,
	PRIMARY KEY (partitionKey, clusterKey)) WITH CLUSTERING ORDER BY (clusterKey ASC);
	                                                                                              
INSERT INTO demo_keyspace.parent_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'parent_cluster1', 'parent_text');
INSERT INTO demo_keyspace.parent_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'parent_cluster2', 'parent_text');
INSERT INTO demo_keyspace.parent_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'parent_cluster3', 'parent_text');
INSERT INTO demo_keyspace.parent_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'parent_cluster4', 'parent_text');
INSERT INTO demo_keyspace.parent_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'parent_cluster5', 'parent_text');
INSERT INTO demo_keyspace.parent_entity (partitionKey, clusterKey, text) VALUES ('partition2', 'parent_cluster1', 'parent_text');
INSERT INTO demo_keyspace.parent_entity (partitionKey, clusterKey, text) VALUES ('partition2', 'parent_cluster2', 'parent_text');
INSERT INTO demo_keyspace.parent_entity (partitionKey, clusterKey, text) VALUES ('partition2', 'parent_cluster3', 'parent_text');
INSERT INTO demo_keyspace.parent_entity (partitionKey, clusterKey, text) VALUES ('partition2', 'parent_cluster4', 'parent_text');

INSERT INTO demo_keyspace.child_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'child_cluster1', 'text');
INSERT INTO demo_keyspace.child_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'child_cluster2', 'text');
INSERT INTO demo_keyspace.child_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'child_cluster3', 'text');
INSERT INTO demo_keyspace.child_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'child_cluster4', 'text');
INSERT INTO demo_keyspace.child_entity (partitionKey, clusterKey, text) VALUES ('partition1', 'child_cluster5', 'text');
INSERT INTO demo_keyspace.child_entity (partitionKey, clusterKey, text) VALUES ('partition2', 'child_cluster1', 'text');
INSERT INTO demo_keyspace.child_entity (partitionKey, clusterKey, text) VALUES ('partition2', 'child_cluster2', 'text');
INSERT INTO demo_keyspace.child_entity (partitionKey, clusterKey, text) VALUES ('partition2', 'child_cluster3', 'text');
INSERT INTO demo_keyspace.child_entity (partitionKey, clusterKey, text) VALUES ('partition2', 'child_cluster4', 'text');