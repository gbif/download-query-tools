package org.gbif.occurrence.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HumanPredicateBuilderTest {

  private HumanPredicateBuilder builder;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  public void init() {
    TitleLookupServiceImpl tl = mock(TitleLookupServiceImpl.class);
    when(tl.getDatasetTitle(ArgumentMatchers.any())).thenReturn("The little Mermaid");
    when(tl.getSpeciesName(ArgumentMatchers.any())).thenReturn("Abies alba Mill.");
    builder = new HumanPredicateBuilder(tl);
  }

  @Test
  public void testHumanPredicateStringNull() {
    String s = null;
    Predicate p = null;
    builder.humanFilter(p);
    assertEquals("{ }", builder.humanFilterString(p));
    assertEquals("{ }", builder.humanFilterString(s));
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

      assertEquals(expected.trim(), actual.trim());
    }
  }

  @Test
  public void testTooManyLookups() {
    // If there are more than 10,050 lookups (dataset, taxa) give up; it's likely to be too slow.

    List<String> bigList = new ArrayList<>();
    for (int i = 0; i<11000; i++) {
      bigList.add(""+i);
    }
    Predicate bigIn = new InPredicate(OccurrenceSearchParameter.TAXON_KEY, bigList, false);

    try {
      builder.humanFilter(bigIn);
      fail();
    } catch (IllegalStateException e) {}

    try {
      builder.humanFilterString(bigIn);
      fail();
    } catch (IllegalStateException e) {}
  }
}
