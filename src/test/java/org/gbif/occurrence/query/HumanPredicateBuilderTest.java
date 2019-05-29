package org.gbif.occurrence.query;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HumanPredicateBuilderTest {

  private HumanPredicateBuilder builder;
  private ObjectMapper mapper = new ObjectMapper();

  @Before
  public void init() {
    TitleLookup tl = mock(TitleLookup.class);
    when(tl.getDatasetTitle(Matchers.any())).thenReturn("The little Mermaid");
    when(tl.getSpeciesName(Matchers.any())).thenReturn("Abies alba Mill.");
    builder = new HumanPredicateBuilder(tl);
  }

  @Test
  public void humanPredicateFilterTest() throws Exception {
    String expected = new String(Files.readAllBytes(Paths.get(getClass().getClassLoader().getResource("result.txt").getPath())));
    try (Stream<String> stream = Files.lines(Paths.get(getClass().getClassLoader().getResource("source.txt").getPath()))) {
      String actual = stream
          .map(p -> p.substring(1, p.length() - 1))
          .map(stringPredicate -> {
            try {
              mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readTree(stringPredicate));
              return builder.humanFilterString(stringPredicate);
            } catch (Exception e) {
              System.out.println("there is an exception");
              return null;
            }
          }).collect(Collectors.joining("\n---------------------------------------------\n"));

      assertEquals(expected, actual);
    }
  }
}
