package org.gbif.occurrence.query;

import org.gbif.api.model.Constants;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.registry.Dataset;

import java.util.UUID;

import com.sun.jersey.api.client.WebResource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TitleLookupTest {

  @Test
  public void testLookup() throws Exception {
    WebResource res = mock(WebResource.class);
    when(res.path(anyString())).thenReturn(res);

    NameUsage abies = new NameUsage();
    abies.setScientificName("Abies alba Mill.");
    when(res.get(NameUsage.class)).thenReturn(abies);

    Dataset pontaurus = new Dataset();
    pontaurus.setTitle("PonTaurus");
    when(res.get(Dataset.class)).thenReturn(pontaurus);

    TitleLookup tl = new TitleLookup(res);

    assertEquals("Abies alba Mill.", tl.getSpeciesName("4231"));
    assertEquals("PonTaurus", tl.getDatasetTitle(UUID.randomUUID().toString()));
  }

  @Test
  //@Ignore("manual API test only")
  public void integrationTestLookup() throws Exception {
    TitleLookup tl = new TitleLookup("http://api.gbif.org/v1/");
    assertEquals("Aves", tl.getSpeciesName("212"));
    assertEquals("GBIF Backbone Taxonomy", tl.getDatasetTitle(Constants.NUB_DATASET_KEY.toString()));
  }

}