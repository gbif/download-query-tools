# Download Query Tools
Utility classes to handle common aspects of GBIF download REST services. The public classes in this library provide:

  * HumanFilterBuilder: translates download query predicates into human-readable strings.
  * PredicateGeometryPointCounter, PredicateTaxonCounter: count points/taxa to assess predicate complexity.
  * QueryParameterFilterBuilder: translates download query predicates into a flat query string that is suitable for HTTP links.
  * TitleLookup: Utility ws-client class to get dataset and species titles used in downloads.

## To build the project

Execute the Maven command:
```
mvn clean package verify install
```
