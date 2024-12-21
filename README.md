# 3dcitykg
3DCityKG: Knowledge Graph representation for Semantic 3D City Models

This repository provides a fully working implementation of the mapping of semantic 3D city models that are compliant to the [CityGML 1.0/2.0/3.0 standard](https://www.ogc.org/de/publications/standard/citygml/) onto rich graph representations in the [graph database system Neo4j](https://neo4j.com). It has been used already in different projects like change detection and interpretation between two versions of the same city model (project [citymodel-compare](https://github.com/tum-gis/citymodel-compare)) as well as in [multi-modal route planning using CityGML 3.0](https://github.com/tum-gis/citygml3-multimodal-routing) (navigating from a specific room inside one building to a specific room in another building and traveling by car in-between including finding a parking space and the pedestrian route planning from the parking space to the target room in the destination building).

The CityGML import/export function of 3DCityKG supports reading and writing CityGML-compliant datasets given in the XML-based CityGML encodings of CityGML version 1.0, 2.0, and 3.0 as well as the CityJSON encoding. Imported data can be analysed, modified, and enriched using Neo4j's Cypher query language. The mapped 3D city models can interactively be explored using the Neo4j Browser, which is part of the Neo4j software ecosystem. 3DCityKG is also supplied as a **Docker container** for quick and easy installation. 

This repository was split from the bigger project [citymodel-compare](https://github.com/tum-gis/citymodel-compare). It is now a stand-alone library that can be used for simply creating a graph representation of a single city model as a basis for further graph-based analysis.

### Features

✔ Map 3D city models to knowledge graphs using Neo4j

✔ Support import for **CityGML 1.0**, **2.0**, **3.0**, and **CityJSON**

  - For **all modules** and **LoDs**
  - Including **XLinks** and implicit geometries
  - **Fast**, **multi-threaded** import for CityGML

✔ Support export for **CityGML 1.0**, **2.0**, **3.0**, and **CityJSON**

  - For **all modules** and **LoDs**
  - CityGML datasets can be exported using a **bounding box**

### Run as a Docker Container

This tool can be used in two ways: either via Gradle or via Docker. For testing purposes, Docker is recommended.

What is needed:

+ [Docker](https://docs.docker.com/get-docker/)
+ [Neo4j Desktop](https://neo4j.com/download/)

1. Make sure Docker is up and running.

2. Pull the following image from Docker Hub:

   ```shell
   docker pull tumgis/3dcitykg:1.0.0
   ```

3. Run the image:

   ```shell
   # Linux 
   docker run \
      -it --rm \
      -p 7474:7474 -p 7687:7687 \
   tumgis/3dcitykg:1.0.0
   ```
   ```shell
   # Windows
    docker run ^
       -it --rm ^
       -p 7474:7474 -p 7687:7687 ^
    tumgis/3dcitykg:1.0.0
   ```

   This will start a Neo4j instance with all necessary dependencies installed. The parameters are as follows:
   + `-it`: Interactive mode.
   + `--rm`: Remove the container after it exits.
   + `-p 7474:7474`: Expose port 7474 of the container to port 7474 of the host machine. This is the port used by the
     Neo4j browser (such as visualization and inspecting Cypher queries).
   + `-p 7687:7687`: Expose port 7687 of the container to port 7687 of the host machine. This is the port used by the
     Neo4j Bolt connector (such as for RESTful services).

4. The database is available at `neo4j://localhost:7687` (Bolt protocol) and can be visualized using Neo4j Desktop or Neo4j Browser.

**Notes**: To simplify the process for testing purposes, the Docker container has already been loaded with
a test dataset, which is the [FZKHaus datasets](https://www.citygmlwiki.org/index.php?title=KIT_CityGML_Examples).

**Notes**: Once started, the datasets will be automatically mapped and stored in the graph database. All from scratch, no existing Neo4j database instance is contained in the Docker
container beforehand.
Please refer to this [section](#use-own-datasets) for more details on how to use your own datasets.

### Configure the Program

The entire program can be configured using a single file located at `lib/config/run.conf`. Example configurations for CityGML 1.0, 2.0, 3.0, and CityJSON datasets can be found in the `examples` sub-directory.

The most important settings are:

```bash
# USE CASE
# 0: Map only using the configurations in this file
# 1: Export only using an existing database configured in this file
# 2: Map AND Export using the configurations in this file
case = 2

# DATABASE CONFIGURATIONS
# .......................

# Keep database online
db.online = true

# MAPPER CONFIGURATIONS
# .....................

# Input dataset to map onto graphs, can have multiple files/directories
# If a path is a directory, ALL files in that folder shall be imported as one
mapper.dataset.paths = [
  "input/citygml/fzk_haus_lod2_v2.gml"
]

# CITYGML CONFIGURATIONS
# ......................

# CityGML version
citygml.version = "v2_0"
# OR CityJSON
cityjson = false

# CityGML export
citygml.export.partition = 0
citygml.export.bbox = "457842.0, 5439083.0, 111.8, 457854.0, 5439093.0, 118.3"
citygml.export.path = "output/citygml/export_v2.gml"
```

### Use Own Datasets

1. Make sure Docker is up and running.

2. Pull the following image from Docker Hub:
   ```shell
   docker pull tumgis/3dcitykg:1.0.0
   ```
   
3. Clone the project:
   ```shell
   git clone https://github.com/tum-gis/3dcitykg
   ```
   
4. Place your CityGML or CityJSON datasets in the `input/citygml` directory:
   ```shell
   lib
   ├── input
   │   └── citygml
   │       ├── dataset1.gml
   │       ├── dataset2.gml
   │       └── ...
   │   └── cityjson
   │       ├── dataset1.json
   │       └── ...
   ```
   ```shell
   # Example
   cp /path/to/dataset.gml lib/input/citygml/
   ```
5. Configure the program using the `lib/config/run.conf` file:
   ```bash
   mapper.dataset.paths = [
     "input/citygml/dataset1.gml"
   ]
   ```
   
6. Bind the `input`, `output`, and `config` directory from the host machine to the Docker container:
   ```shell
    # Linux
    docker run \
        -it --rm \
        -p 7474:7474 -p 7687:7687 \
        -v "/absolute/path/to/config:/home/gradle/src/3dcitykg/config" \
        -v "/absolute/path/to/input:/home/gradle/src/3dcitykg/input" \
        -v "/absolute/path/to/output:/home/gradle/src/3dcitykg/output" \
    tumgis/3dcitykg:1.0.0
    ```

    ```shell
    # Windows
    docker run ^
        -it --rm ^
        -p 7474:7474 -p 7687:7687 ^
        -v "/absolute/path/to/config:/home/gradle/src/3dcitykg/config" ^
        -v "/absolute/path/to/input:/home/gradle/src/3dcitykg/input" ^
        -v "/absolute/path/to/output:/home/gradle/src/3dcitykg/output" ^
    tumgis/3dcitykg:1.0.0
    ```

7. The exported datasets will be available in the `/absolute/path/to/output` directory on the local host machine.

### Build Docker Image

To build the Docker image from scratch, run the following command:

```shell
docker build --no-cache -t tumgis/3dcitykg:1.0.0 .
```

Tag and push the image to Docker Hub:

```shell
docker tag 3dcitykg tumgis/3dcitykg:1.0.0
docker push tumgis/3dcitykg:1.0.0
```

## Publications

This tool is part of the following publications:

**Nguyen, Son H.**: [_Automatic Detection and Interpretation of Changes in Massive Semantic 3D City Models_](https://mediatum.ub.tum.de/?id=1748695).
Dissertation, Technical University of Munich, 2024.

**Nguyen, Son H.; Kolbe, Thomas H.**: [_Identification and Interpretation of Change Patterns in
Semantic 3D City Models_](https://mediatum.ub.tum.de/doc/1730733/1730733.pdf). Lecture Notes in Geoinformation and Cartography - Recent Advances in 3D Geoinformation Science -
Proceedings of the 18th 3D GeoInfo Conference, Springer, 2023.

**Nguyen, Son H.; Kolbe, Thomas H.**: [_Path-tracing Semantic Networks to Interpret Changes in Semantic 3D City
Models_](https://www.isprs-ann-photogramm-remote-sens-spatial-inf-sci.net/X-4-W2-2022/217/2022/).
Proceedings of the 17th International 3D GeoInfo Conference 2022 (ISPRS Annals of the Photogrammetry, Remote Sensing and
Spatial Information Sciences), ISPRS, 2022.

**Nguyen, Son H.; Kolbe, Thomas H.**: [_Modelling Changes, Stakeholders and their Relations in Semantic 3D City
Models_](https://www.isprs-ann-photogramm-remote-sens-spatial-inf-sci.net/VIII-4-W2-2021/137/2021/).
Proceedings of the 16th International 3D GeoInfo Conference 2021 (ISPRS Annals of the Photogrammetry, Remote Sensing and
Spatial Information Sciences), ISPRS, 2021, 137-144.

**Nguyen, Son H.; Kolbe, Thomas H.**: [_A Multi-Perspective Approach to Interpreting Spatio-Semantic Changes of Large 3D
City
Models in CityGML using a Graph
Database_](https://www.isprs-ann-photogramm-remote-sens-spatial-inf-sci.net/VI-4-W1-2020/143/2020/). Proceedings of the
15th International 3D GeoInfo Conference 2020 (ISPRS
Annals
of the Photogrammetry, Remote Sensing and Spatial Information Sciences), ISPRS, 2020, 143–150.

**Nguyen, Son H.; Yao, Zhihang; Kolbe, Thomas H.**: [_Spatio-Semantic Comparison of Large 3D City Models in CityGML
Using
a Graph
Database_](https://gispoint.de/artikelarchiv/gis/2018/gisscience-ausgabe-32018/4612-raeumlich-semantischer-vergleich-grosser-3d-stadtmodelle-in-citygml-unter-verwendung-einer-graph-datenbank.html).
gis.Science (3/2018), 2018, 85-100.

**Nguyen, Son H.; Yao, Zhihang; Kolbe, Thomas H.**: [_Spatio-Semantic Comparison of Large 3D City Models in CityGML
Using a Graph Database_](https://mediatum.ub.tum.de/doc/1425153/1425153.pdf). Proceedings of the 12th International 3D
GeoInfo Conference 2017 (ISPRS Annals of the Photogrammetry, Remote Sensing and Spatial Information Sciences), ISPRS,
2017, 99-106.

**Nguyen, Son H.**: [_Spatio-semantic Comparison of 3D City Models in CityGML using a Graph
Database_](https://mediatum.ub.tum.de/doc/1374646/1374646.pdf). Master thesis, 2017.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgement

The development of these methods and implementations were supported
and partially funded by the company [CADFEM](https://www.cadfem.net/)
within a dedicated collaboration project in the context of the
[Leonhard Obermeyer Center (LOC)](https://www.ed.tum.de/loc)
at the [Technical University of Munich (TUM)](https://www.tum.de).

## Contact

If you have any questions or suggestions, please contact:

**Son H. Nguyen** 

Chair of Geoinformatics | Department of Aerospace and Geodesy

TUM School of Engineering and Design | Technical University of Munich (TUM)

Arcisstr. 21, 80333 Munich, Germany

Email: [son.nguyen@tum.de](mailto:son.nguyen@tum.de)

[Homepage](https://www.asg.ed.tum.de/en/gis/our-team/staff/son-h-nguyen/) | [LinkedIn](https://www.linkedin.com/in/son-h-nguyen/) | [ResearchGate](https://www.researchgate.net/profile/Son-Nguyen-101)
