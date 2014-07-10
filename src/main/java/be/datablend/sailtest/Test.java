

package be.datablend.sailtest;

import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;
import org.openrdf.model.Resource;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;


import java.io.IOException;

/**
 * User: dsuvee
 * Date: 15/07/11
 */
public class Test {

    private Neo4jGraph graph = null;
    private GraphSail sail = null;
    private RepositoryConnection connection = null;

    public void openSailConnection() throws SailException, RepositoryException {
        // Create the sail graph database
        graph = new MyNeo4jGraph("var/flights", 100000);

        sail = new GraphSail(graph);

        // Initialize the sail store
        sail.initialize();

        // Get the sail repository connection
        connection = new SailRepository(sail).getConnection();
        connection.setAutoCommit(false);
    }

    public void closeSailConnection() throws SailException {
        sail.shutDown();
        graph.shutdown();
    }

    public void importData() throws SailException, RepositoryException, IOException, RDFParseException {
        System.out.println("Import started ...");
        // Import the data
        connection.add(getClass().getClassLoader().getResource("sneeair.rdf"), null, RDFFormat.RDFXML);
        System.out.println("Import stopped ...");
    }

    public void findFlightData() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        System.out.println("Execution some sparql query ...");
        // Create a query for all flights with a duration of 1 hour and 35 minutes
        TupleQuery durationquery = connection.prepareTupleQuery(QueryLanguage.SPARQL,
                "PREFIX io: <http://www.daml.org/2001/06/itinerary/itinerary-ont#> " +
                "PREFIX fl: <http://www.snee.com/ns/flights#> " +
                "SELECT ?number ?departure ?destination " +
                "WHERE { " +
                        "?flight io:flight ?number . " +
                        "?flight fl:flightFromCityName ?departure . " +
                        "?flight fl:flightToCityName ?destination . " +
                        "?flight io:duration \"1:35\" . " +
                "}");
        System.out.println("Printing sparql query results ....");
        TupleQueryResult result = durationquery.evaluate();


        while (result.hasNext()) {
            BindingSet binding = result.next();
            System.out.println(binding.getBinding("number").getValue() + " " + binding.getBinding("departure").getValue() + " " + binding.getBinding("destination").getValue());
        }
    }

    public static void main(String[] args) throws RepositoryException, IOException, SailException, RDFParseException, MalformedQueryException, QueryEvaluationException {
        Test test = new Test();
        // Open the sail connection
        test.openSailConnection();

        // Import the flight data
        test.importData();

        // Execute a sparql query
        test.findFlightData();

        // Close sail connection
        test.closeSailConnection();
    }

}
