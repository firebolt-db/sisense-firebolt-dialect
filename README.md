# Firebolt SQL dialect for Sisense

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=firebolt-db_sisense-firebolt-driver&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=firebolt-db_sisense-firebolt-driver)

This repo contains the [dialect file](src/main/java/FireboltSqlDialect.java) needed for Sisense to generate appropriate SQL on live connections to Firebolt.
It is a slightly modified version of [dialect implementation](https://github.com/apache/calcite/blob/724eb032d0141c15d17422d50c5235be00ac989f/core/src/main/java/org/apache/calcite/sql/dialect/FireboltSqlDialect.java) that was written for [Apache Calcite](https://github.com/apache/calcite) as it needs to be a valid java class that compile.

