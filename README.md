# neo4j-gedcom

[![CI](https://github.com/dhrudevalia/neo4j-gedcom/actions/workflows/ci.yml/badge.svg)](https://github.com/dhrudevalia/neo4j-gedcom/actions/workflows/ci.yml)

This project hosts the Cypher procedure `genealogy.loadGedcom()` related to the import of genealogical data, encoded in Gedcom (5.5) files.

## Download

Proceed to https://github.com/neo4j-contrib/neo4j-gedcom/packages/ and grab the latest published JAR file.

## Build Locally

### Prerequisites

 - [JDK 17](https://adoptium.net/temurin/releases/?version=17)
 - [Apache Maven](https://maven.apache.org/download.cgi)

### Steps

```shell
mvn package -DskipTests
```

The resulting JAR file must be in `target/`, whose name conforms to the following structure `neo4j-gedcom-${version}.jar` (e.g.: `neo4j-gedcom-1.0-SNAPSHOT.jar`).

## How-To

### Prerequisites

Grab the JAR (see previous section).

Make sure you have access and you are able to alter the target Neo4j server's configuration (often `/path/to/server/conf/neo4j.conf`).

`/path/to/server` will be omitted from now on.

### Deployment

First, the JAR must be placed under the directory configured for `server.directories.plugins` (`plugins/` by default).

The Gedcom file(s) must be placed under the directory configured for `server.directories.import` (`import/` by default).

Then, you need to register the procedure as "unrestricted", since it needs disk access to the Gedcom files you want to import.

For this to happen, `dbms.security.procedures.unrestricted` needs to be updated with the procedure name appended after the existing value.

For instance, this setting:

```ini
dbms.security.procedures.unrestricted=jwt.security.*
```

then becomes:

```ini
dbms.security.procedures.unrestricted=jwt.security.*,genealogy.loadGedcom
```

Finally, restart your Neo4j server.

Now, you should be able to open your favorite Cypher tool and run:

```cypher
CALL genealogy.loadGedcom('my.ged')
```

In this invocation example, 'my.ged' is a Gedcom 5.5 file placed under `/path/to/server/import/my.ged`.
