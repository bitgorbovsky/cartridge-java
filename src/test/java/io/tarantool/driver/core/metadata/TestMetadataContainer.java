package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolIndexPartMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataContainer;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Alexey Kuzin
 */
public class TestMetadataContainer implements TarantoolMetadataContainer {

    private final TarantoolSpaceMetadataImpl testSpaceMetadata;
    private final TarantoolIndexMetadataImpl testPrimaryIndexMetadata;
    private final TarantoolIndexMetadataImpl testIndexMetadata1;
    private final TarantoolIndexMetadataImpl testIndexMetadata2;
    private final TarantoolIndexMetadataImpl testIndexMetadata3;
    private final TarantoolIndexMetadataImpl testIndexMetadata4;

    public TestMetadataContainer() {
        testSpaceMetadata = new TarantoolSpaceMetadataImpl();
        testSpaceMetadata.setSpaceId(512);
        testSpaceMetadata.setSpaceName("test");
        TarantoolFieldMetadata firstFieldMetadata = new TarantoolFieldMetadataImpl("first", "string", 0);
        TarantoolFieldMetadata secondFieldMetadata = new TarantoolFieldMetadataImpl("second", "number", 1);
        TarantoolFieldMetadata thirdFieldMetadata = new TarantoolFieldMetadataImpl("third", "number", 2);
        TarantoolFieldMetadata fourthFieldMetadata = new TarantoolFieldMetadataImpl("fourth", "number", 3);
        Map<String, TarantoolFieldMetadata> fieldMetadataMap = new HashMap<>();
        fieldMetadataMap.put("first", firstFieldMetadata);
        fieldMetadataMap.put("second", secondFieldMetadata);
        fieldMetadataMap.put("third", thirdFieldMetadata);
        fieldMetadataMap.put("fourth", fourthFieldMetadata);
        testSpaceMetadata.setSpaceFormatMetadata(fieldMetadataMap);

        testPrimaryIndexMetadata = new TarantoolIndexMetadataImpl();
        testPrimaryIndexMetadata.setIndexId(0);
        testPrimaryIndexMetadata.setIndexName("primary");
        testPrimaryIndexMetadata.setSpaceId(512);
        testPrimaryIndexMetadata.setIndexParts(Collections.singletonList(
            new TarantoolIndexPartMetadataImpl(0, "string"))
        );

        testIndexMetadata2 = new TarantoolIndexMetadataImpl();
        testIndexMetadata2.setIndexId(1);
        testIndexMetadata2.setIndexName("asecondary1");
        testIndexMetadata2.setSpaceId(512);
        List<TarantoolIndexPartMetadata> parts = new ArrayList<>();
        parts.add(new TarantoolIndexPartMetadataImpl(1, "number"));
        parts.add(new TarantoolIndexPartMetadataImpl(2, "number"));
        testIndexMetadata2.setIndexParts(parts);

        testIndexMetadata3 = new TarantoolIndexMetadataImpl();
        testIndexMetadata3.setIndexId(2);
        testIndexMetadata3.setIndexName("secondary2");
        testIndexMetadata3.setSpaceId(512);
        parts = new ArrayList<>();
        parts.add(new TarantoolIndexPartMetadataImpl(1, "number"));
        parts.add(new TarantoolIndexPartMetadataImpl(3, "number"));
        testIndexMetadata3.setIndexParts(parts);

        testIndexMetadata4 = new TarantoolIndexMetadataImpl();
        testIndexMetadata4.setIndexId(3);
        testIndexMetadata4.setIndexName("asecondary3");
        testIndexMetadata4.setSpaceId(512);
        parts = new ArrayList<>();
        parts.add(new TarantoolIndexPartMetadataImpl(1, "number"));
        parts.add(new TarantoolIndexPartMetadataImpl(2, "number"));
        parts.add(new TarantoolIndexPartMetadataImpl(3, "number"));
        testIndexMetadata4.setIndexParts(parts);

        testIndexMetadata1 = new TarantoolIndexMetadataImpl();
        testIndexMetadata1.setIndexId(4);
        testIndexMetadata1.setIndexName("asecondary");
        testIndexMetadata1.setSpaceId(512);
        testIndexMetadata1.setIndexParts(Collections.singletonList(new TarantoolIndexPartMetadataImpl(1, "number")));
    }

    @Override
    public Map<String, TarantoolSpaceMetadata> getSpaceMetadataByName() {
        Map<String, TarantoolSpaceMetadata> spaceMetadataByName = new HashMap<>();
        spaceMetadataByName.put("test", testSpaceMetadata);
        return spaceMetadataByName;
    }

    @Override
    public Map<String, Map<String, TarantoolIndexMetadata>> getIndexMetadataBySpaceName() {
        Map<String, TarantoolIndexMetadata> indexes = new HashMap<>();
        indexes.put("primary", testPrimaryIndexMetadata);
        indexes.put("asecondary", testIndexMetadata1);
        indexes.put("asecondary1", testIndexMetadata2);
        indexes.put("secondary2", testIndexMetadata3);
        indexes.put("asecondary3", testIndexMetadata4);

        Map<String, Map<String, TarantoolIndexMetadata>> indexMetadataBySpaceName = new HashMap<>();
        indexMetadataBySpaceName.put("test", indexes);
        return indexMetadataBySpaceName;
    }
}
