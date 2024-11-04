package citykg.core.ref;

import citykg.core.factory.AuxNodeLabels;
import citykg.core.factory.AuxPropNames;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.StreamSupport;

// Represents a uniquely identifiable "root" node of a subgraph in neo4j
public class Neo4jRef extends GraphRef {
    private final Label label;
    private final static Logger logger = LoggerFactory.getLogger(Neo4jRef.class);

    public Neo4jRef(Node rootNode) {
        super();
        if (!(rootNode.getLabels().iterator().hasNext()))
            throw new RuntimeException("Could not create a UUID of non-labelled nodes");
        label = StreamSupport.stream(rootNode.getLabels().spliterator(), false)
                .filter(l -> !l.toString().contains(AuxNodeLabels.__PARTITION_INDEX__.toString()))
                .iterator().next();
        // Also set this to the node for later queries
        rootNode.setProperty(AuxPropNames.__UUID__.toString(), uuid);
    }

    public Neo4jRef(Neo4jRef that) {
        super(that);
        label = that.label;
    }

    public Neo4jRef(String uuid, Label label) {
        super(uuid);
        this.label = label;
    }

    public Node getRepresentationNode(Transaction tx) {
        return tx.findNode(label, AuxPropNames.__UUID__.toString(), uuid);
    }

    public Label getLabel() {
        return label;
    }
}
