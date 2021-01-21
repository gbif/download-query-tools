package org.gbif.occurrence.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TitleLookupTest {

  @Test
  public void lookupDatasetTitleTest() {
    TitleLookupService service =
        TitleLookupServiceFactory.getInstance("http://api.gbif-dev.org/v1/");

    assertEquals("FBIP: Identification of viruses infecting indigenous ornamental bulbous plants in South Africa using NGS",
        service.getDatasetTitle("25be374b-8318-4a8c-ae3c-7ae592233afb"));
    assertEquals("FBIP: Mycorrhizal associations of Erica hair roots in South African fynbos",
        service.getDatasetTitle("417276eb-5082-4da3-b410-fe5680da65d9"));
    assertEquals("FBIP: plantae_magnoliophyta_collection",
        service.getDatasetTitle("7abad3fc-068a-4f34-8b3d-bfe03155ccde"));
  }

  @Test
  public void lookupSpeciesNameTest() {
    TitleLookupService service =
        TitleLookupServiceFactory.getInstance("http://api.gbif-dev.org/v1/");

    assertEquals("incertae sedis", service.getSpeciesName("0"));
    assertEquals("Animalia", service.getSpeciesName("1"));
    assertEquals("Archaea", service.getSpeciesName("2"));
  }
}
