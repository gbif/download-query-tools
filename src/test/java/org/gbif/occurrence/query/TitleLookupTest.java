package org.gbif.occurrence.query;

import com.sun.jersey.api.client.WebResource;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.registry.Dataset;
import org.junit.Test;

import java.util.UUID;

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

    TitleLookupServiceImpl tl = new TitleLookupServiceImpl(res);

    assertEquals("Abies alba Mill.", tl.getSpeciesName("4231"));
    assertEquals("PonTaurus", tl.getDatasetTitle(UUID.randomUUID().toString()));
  }
}