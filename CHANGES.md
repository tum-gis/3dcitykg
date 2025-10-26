# Change Log

### v1.1.0 [Pending]

##### NEW

+ Added an additional start case number `3` in the config file for opening existing database (without mapping or exporting). 
  This is useful for quickly deploying and querying an existing database
+ APOC core is now automatically included when running via Docker
+ Enabled security settings for APOC libraries

##### CHANGES

+ Introduced the tag `latest` that references the newest version of the Docker image
+ Disabled automatic Neo4j data usage report by default. Users can reenable it by setting `dbms.usage_report.enabled=true` in `config/neo4j.conf`.

### v1.0.0 - [Released](https://github.com/tum-gis/3dcitykg/releases/tag/v1.0.0)