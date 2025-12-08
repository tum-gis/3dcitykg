# Change Log

### v1.1.0 [Released](https://github.com/tum-gis/3dcitykg/releases/tag/v1.1.0)

##### NEW

+ Added an additional start case number `3` in the config file for opening existing database (without mapping or exporting). 
  This is useful for quickly deploying and querying an existing database
+ APOC core is now automatically included when running via Docker
+ Enabled security settings for APOC libraries
+ Allow empty bounding box in run.conf when exporting entire dataset

##### CHANGES

+ Introduced the tag `latest` that references the newest version of the Docker image (`tumgis/3dcitykg:latest`)
+ Disabled automatic Neo4j data usage report by default. Users can reenable it by setting `dbms.usage_report.enabled=true` in `config/neo4j.conf`
+ Updated to Gradle 9, Neo4j 5 using Java JDK 17
+ Updated citygml4j

### v1.0.0 - [Released](https://github.com/tum-gis/3dcitykg/releases/tag/v1.0.0)