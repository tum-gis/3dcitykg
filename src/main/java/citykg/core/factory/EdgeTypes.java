package citykg.core.factory;

import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;

public enum EdgeTypes implements RelationshipType {
    elementData,
    cityObjectMember,
    object,
    lod2MultiSurface,
    surfaceMember,
    boundedBy,
    envelope,
    lowerCorner,
    upperCorner,
    value,

    clazz,
    species,
    height,
    name;

    public static boolean isIn(RelationshipType type) {
        return Arrays.stream(values()).anyMatch(t -> type.name().startsWith(t.name()) || type.name().endsWith(t.name()));
    }
}
