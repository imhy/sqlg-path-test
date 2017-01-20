import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.Map;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.Pop.last;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;


public class PathTest {

    private static final Logger LOG = LoggerFactory.getLogger(PathTest.class);

    @Test
    public void pathTestSqlg() {

        Configuration configuration = new BaseConfiguration();
        configuration.addProperty("jdbc.url", "jdbc:postgresql://localhost:5432/sqlg_test");
        configuration.addProperty("jdbc.username", "svs");
        configuration.addProperty("jdbc.password", "svs");

        Graph graph = SqlgGraph.open(configuration);
        GraphTraversalSource g = graph.traversal();

        GraphTraversal traversal = getTraversal(g);

        LOG.info("start traversal, vertex count: {}", g.V().count().next());

        while (traversal.hasNext()) {
            LOG.info(traversal.next().toString());
        }
    }

    @Test
    public void pathTestTinkerGraph() {

        Configuration config = new BaseConfiguration();
        config.setProperty("gremlin.graph", "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph");

        Graph graph = GraphFactory.open(config);
        GraphTraversalSource g = graph.traversal();
        loadData(graph);
        GraphTraversal traversal = getTraversal(g);

        LOG.info("start traversal, vertex count: {}", g.V().count().next());

        while (traversal.hasNext()) {
            LOG.info(traversal.next().toString());
        }
    }

    public GraphTraversal getTraversal(GraphTraversalSource g) {
        Function timeAtWarehouse = new Function() {
            @Override
            public Long apply(Object o) {
                Map m = (Map) o;
                Integer dow = ((Edge) (m.get("prev"))).value("dow");
                Long readyTime = ((Edge) (m.get("prev"))).value("readyTime");
                Integer depDow = ((Edge) (m.get("curr"))).value("dow");
                Long depTime = ((Edge) (m.get("curr"))).value("depTime");
                return (depTime - readyTime) >= 0 ? (depTime - readyTime) : Long.MAX_VALUE;
            }
        };

        return g.V().outE("tsw").as("e").inV().emit().repeat(
                __.flatMap(
                        __.outE("tsw").filter(__.as("edge").select(last, "e").where(P.eq("edge")).by("speed")).
                                group().by(__.inV()).by(__.project("curr", "prev").by().by(__.select(last, "e")).fold()).
                                select(Column.values).unfold().order(local).by(timeAtWarehouse).limit(local, 1).select("curr")
                ).as("e").inV().simplePath()
        ).times(20).map(__.union((Traversal) __.select(last, "e").by("speed"), (Traversal) __.path()).fold());
    }

    @Test
    public void loadDataSqlg() {
        Configuration configuration = new BaseConfiguration();
        configuration.addProperty("jdbc.url", "jdbc:postgresql://localhost:5432/sqlg_test");
        configuration.addProperty("jdbc.username", "svs");
        configuration.addProperty("jdbc.password", "svs");

        Graph graph = SqlgGraph.open(configuration);

        graph.tx().open();
        loadData(graph);
        graph.tx().commit();
    }

    public void loadData(Graph graph) {

        Vertex v0 = graph.addVertex("code", "0");
        Vertex v1 = graph.addVertex("code", "1");
        Vertex v2 = graph.addVertex("code", "2");
        Vertex v3 = graph.addVertex("code", "3");


        v0.addEdge("tsw", v1, "speed", "1", "readyTime", 10l, "depTime", 5l, "dow", 1);
        v1.addEdge("tsw", v2, "speed", "1", "readyTime", 15l, "depTime", 9l, "dow", 1); //must be ignored in longest path
        v1.addEdge("tsw", v2, "speed", "1", "readyTime", 20l, "depTime", 17l, "dow", 1); //must be used in longest path
        v2.addEdge("tsw", v3, "speed", "1", "readyTime", 30l, "depTime", 25l, "dow", 1);
        v1.addEdge("tsw", v2, "speed", "2", "readyTime", 28l, "depTime", 23l, "dow", 1); //speed 2
    }
}
