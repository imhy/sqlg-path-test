import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.Pop.last;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;


public class PathTest {

	private static final Logger LOG = LoggerFactory.getLogger(PathTest.class);
	private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/sqlg_test";
	private static final String JDBC_USERNAME = "svs";
	private static final String JDBC_PASSWORD = "svs";

	@Test
	public void pathsShouldBeTheSameTest() {
		List<SinglePath> sqlgGraphResult = pathsSqlgGraph();
		List<SinglePath> tinkerGraphResult = pathsTinkerGraph();

		Assert.assertNotNull(sqlgGraphResult);
		Assert.assertNotNull(tinkerGraphResult);
		printResult("SqlgGraph", sqlgGraphResult);
		printResult("ThinkerGraph", tinkerGraphResult);

		boolean theSame = compareResults(tinkerGraphResult, sqlgGraphResult);
		Assert.assertTrue(theSame);
	}

	private List<SinglePath> pathsSqlgGraph() {
		Configuration configuration = getSqlgConfiguration();
		List<SinglePath> paths = null;

		try (Graph graph = SqlgGraph.open(configuration)) {
			GraphTraversalSource g = graph.traversal();

			GraphTraversal traversal = getTraversal(g);

			LOG.info("start traversal SqlgGraph, vertex count: {}", g.V().count().next());
			paths = getResult(traversal);

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		return paths;
	}

	private List<SinglePath> getResult(GraphTraversal traversal) {
		List<SinglePath> paths = new ArrayList<>();
		while (traversal.hasNext()) {
			SinglePath sp = new SinglePath(traversal.next());
			paths.add(sp);
		}
		return paths;
	}

	private void printResult(String label, List<SinglePath> paths) {
		LOG.info("Label: {}", label);
		if (paths == null) {
			LOG.info("Result is null");
		} else if (paths.isEmpty()) {
			LOG.info("Result is empty");
		} else {
			paths.stream().forEach(p -> LOG.info(p.toString()));
		}
	}


	private List<SinglePath> pathsTinkerGraph() {

		Configuration config = new BaseConfiguration();
		config.setProperty("gremlin.graph", "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph");

		Graph graph = GraphFactory.open(config);
		GraphTraversalSource g = graph.traversal();
		loadData(graph);
		GraphTraversal traversal = getTraversal(g);

		LOG.info("start traversal ThinkerGraph, vertex count: {}", g.V().count().next());
		return getResult(traversal);
	}

	private GraphTraversal getTraversal(GraphTraversalSource g) {
		Function timeAtWarehouse = (Object o) -> {
			Map m = (Map) o;
			Long readyTime = ((Edge) (m.get("prev"))).value("readyTime");
			Long depTime = ((Edge) (m.get("curr"))).value("depTime");
			return (depTime - readyTime) >= 0 ? (depTime - readyTime) : Long.MAX_VALUE;
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

		Configuration configuration = getSqlgConfiguration();

		Graph graph = SqlgGraph.open(configuration);

		graph.tx().open();
		loadData(graph);
		graph.tx().commit();
	}

	private void loadData(Graph graph) {

		Vertex v0 = graph.addVertex("code", "v0");
		Vertex v1 = graph.addVertex("code", "v1");
		Vertex v2 = graph.addVertex("code", "v2");
		Vertex v3 = graph.addVertex("code", "v3");


		v0.addEdge("tsw", v1, "speed", "1", "readyTime", 10l, "depTime", 5l, "dow", 1, "code", "e0");
		v1.addEdge("tsw", v2, "speed", "1", "readyTime", 15l, "depTime", 9l, "dow", 1, "code", "e1"); //must be ignored in longest path
		v1.addEdge("tsw", v2, "speed", "1", "readyTime", 20l, "depTime", 17l, "dow", 1, "code", "e2"); //must be used in longest path
		v2.addEdge("tsw", v3, "speed", "1", "readyTime", 30l, "depTime", 25l, "dow", 1, "code", "e3");
		v1.addEdge("tsw", v2, "speed", "2", "readyTime", 28l, "depTime", 23l, "dow", 1, "code", "e4"); //speed 2
	}

	private Configuration getSqlgConfiguration() {
		Configuration configuration = new BaseConfiguration();
		configuration.addProperty("jdbc.url", JDBC_URL);
		configuration.addProperty("jdbc.username", JDBC_USERNAME);
		configuration.addProperty("jdbc.password", JDBC_PASSWORD);
		return configuration;
	}

	private boolean compareResults(List<SinglePath> etalon, List<SinglePath> check) {
		boolean ok = true;
		if (etalon == null || check == null) return false;

		Map<SinglePath, Integer> countEtalon = etalon.stream().collect(Collectors.toMap(p -> p, p -> Collections.frequency(etalon, p)));
		Map<SinglePath, Integer> countCheck = check.stream().collect(Collectors.toMap(p -> p, p -> Collections.frequency(check, p)));

		List<SinglePath> theSamePaths = etalon.stream().filter(check::contains).collect(Collectors.toList());
		List<SinglePath> wrongPaths = check.stream().filter(e -> !etalon.contains(e)).collect(Collectors.toList());
		List<SinglePath> missingPaths = etalon.stream().filter(e -> !check.contains(e)).collect(Collectors.toList());
		List<SinglePath> wrongCountPaths = theSamePaths.stream().filter(p -> !countEtalon.get(p).equals(countCheck.get(p))).collect(Collectors.toList());
		List<SinglePath> okPaths = theSamePaths.stream().filter(p -> countEtalon.get(p).equals(countCheck.get(p))).collect(Collectors.toList());

		if (!wrongPaths.isEmpty()) {
			printResult("Wrong Paths", wrongPaths);
			ok = false;
		}

		if (!missingPaths.isEmpty()) {
			printResult("Missing Paths", wrongPaths);
			ok = false;
		}

		if (!wrongCountPaths.isEmpty()) {
			printResult("Wrong count Paths", wrongPaths);
			ok = false;
		}

		if (!ok) {
			printResult("Ok Paths", okPaths);
		}

		return ok;
	}

	private class SinglePath {
		private int speed;
		private ImmutablePath immutablePath;
		private String path;

		public SinglePath(Object p) {
			ArrayList<Object> arr = (ArrayList) p;
			this.speed = Integer.valueOf(arr.get(0).toString());
			this.immutablePath = (ImmutablePath) arr.get(1);
			this.path = pathToStr(this.immutablePath);
		}


		public String getPath() {
			return path;
		}

		private String pathToStr(ImmutablePath p) {
			List<Object> objects = p.objects();
			return objects.stream().map(this::elementToCode).collect(Collectors.joining("-"));
		}

		private String elementToCode(Object element) {
			Element e = (Element) element;
			return e.property("code").orElse("n/a").toString();
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("[");
			sb.append("speed: ").append(speed);
			sb.append(", path: ").append(path);
			sb.append(']');
			return sb.toString();
		}

		@Override
		public int hashCode() {
			return path != null ? (String.valueOf(speed) + path).hashCode() : 0;
		}

		@Override
		public boolean equals(Object that) {
			if (that == null) return false;
			if (this == that) return true;
			if (this.getClass() != that.getClass()) return false;
			SinglePath o = (SinglePath) that;
			return (this.path != null && o.getPath() != null && this.path.equals(o.getPath()) && this.speed == o.speed);
		}
	}
}
