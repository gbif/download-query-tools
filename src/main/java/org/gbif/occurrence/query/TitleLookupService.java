package org.gbif.occurrence.query;

public interface TitleLookupService {

  String getDatasetTitle(String datasetKey);

  String getSpeciesName(String usageKey);
}