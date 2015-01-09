package org.gbif.occurrence.query;

import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.client.HttpClient;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TitleLookupModuleTest {

  @Test
  public void testProvideLookup() throws Exception {
    TitleLookupModule mod = new TitleLookupModule(true, "http://api.gbif.org/v1");
    Injector inj = Guice.createInjector(mod);

    TitleLookup tl = inj.getInstance(TitleLookup.class);
    assertNotNull(tl);

    try {
      HttpClient hc = inj.getInstance(HttpClient.class);
      fail("HttpClient should not be exposed");
    } catch (ConfigurationException e) {
      // expected!
    }
  }
}