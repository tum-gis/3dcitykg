package citykg.core.factory;

import java.util.Arrays;

public enum AuxPropNames {
    __UUID__, // Unique IDs to retrieve/rebind graph nodes
    ARRAY_MEMBER_TYPE,
    COLLECTION_MEMBER_TYPE,

    __TYPE__, // Suffix for storing the type of each property value
    __MATCHED__, // Marked as matched in n-to-n matching

    ARRAY_SIZE,
    COLLECTION_SIZE,

    ARRAY_INDEX,
    COLLECTION_INDEX,

    ARRAY_MEMBER,

    modCount,
    size;

    public static boolean isIn(String name) {
        return Arrays.stream(values()).anyMatch(p -> name.startsWith(p.toString()) || name.endsWith(p.toString()));
    }
}
