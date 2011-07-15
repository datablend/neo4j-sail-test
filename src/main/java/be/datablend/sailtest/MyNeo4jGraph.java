package be.datablend.sailtest;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;

import java.util.Map;

/**
 * User: dsuvee
 * Date: 15/07/11
 */
public class MyNeo4jGraph extends Neo4jGraph {

    private long numberOfItems = 0;
    private long maxNumberOfItems = 1;

    public MyNeo4jGraph(final String directory, long maxNumberOfItems) {
        super(directory, null);
        this.maxNumberOfItems = maxNumberOfItems;
    }

    public MyNeo4jGraph(final String directory, final Map<String, String> configuration, long maxNumberOfItems) {
        super(directory, configuration);
        this.maxNumberOfItems = maxNumberOfItems;
    }

    public Vertex addVertex(final Object id) {
        Vertex vertex = super.addVertex(id);
        commitIfRequired();
        return vertex;
    }

    public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
        Edge edge = super.addEdge(id, outVertex, inVertex, label);
        commitIfRequired();
        return edge;
    }

    private void commitIfRequired() {
        // Check whether commit should be executed
        if (++numberOfItems % maxNumberOfItems == 0) {
            // Stop the transaction
            stopTransaction(Conclusion.SUCCESS);
            // Immediately start a new one
            startTransaction();
        }
    }

}
