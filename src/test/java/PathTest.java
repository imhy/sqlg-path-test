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
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.traversal.Pop.last;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;


public class PathTest {

	private static final Logger LOG = LoggerFactory.getLogger(PathTest.class);
	private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/sqlg_test";
	private static final String JDBC_USERNAME = "svs";
	private static final String JDBC_PASSWORD = "svs";
	private static final long VERTICES_COUNT = 4L;
	private static final long EDGES_COUNT = 5L;
	private static final int MAX_PATH_LENGTH = 20;

	@Test
	public void loadDataSqlgTest() {
		Configuration configuration = getSqlgConfiguration();
		GraphTraversalSource g;
		long verticesCount = 0L;
		long edgesCount = 0L;
		try (Graph graph = SqlgGraph.open(configuration)) {
			graph.tx().open();
			loadData(graph);
			graph.tx().commit();
			g = graph.traversal();
			verticesCount = g.V().count().next();
			edgesCount = g.E().count().next();
		} catch (Exception ex) {
			LOG.error(ex.getMessage(), ex);
			Assert.fail(ex.getMessage());
		}

		Assert.assertEquals(VERTICES_COUNT, verticesCount);
		Assert.assertEquals(EDGES_COUNT, edgesCount);
	}

	@Test
	public void pathsShouldBeTheSameTest() {
		List<SinglePath> sqlgGraphResult = pathsSqlgGraph();
		List<SinglePath> tinkerGraphResult = pathsTinkerGraph();

		Assert.assertNotNull(sqlgGraphResult);
		Assert.assertNotNull(tinkerGraphResult);
		printResult("SqlgGraph", sqlgGraphResult);
		printResult("TinkerGraph", tinkerGraphResult);

		boolean theSame = compareResults(tinkerGraphResult, sqlgGraphResult);
		Assert.assertTrue("Paths must be the same.", theSame);
	}

	private List<SinglePath> pathsSqlgGraph() {
		Configuration configuration = getSqlgConfiguration();
		List<SinglePath> paths = null;

		try (Graph graph = SqlgGraph.open(configuration)) {
			GraphTraversalSource g = graph.traversal();
			GraphTraversal traversal = getTraversal(g);
			LOG.info("Start traversal SqlgGraph, vertex count: {}", g.V().count().next());
			paths = getResult(traversal);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			Assert.fail(e.getMessage());
		}
		return paths;
	}

	private List<SinglePath> pathsTinkerGraph() {
		Configuration config = new BaseConfiguration();
		config.setProperty("gremlin.graph", "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph");
		Graph graph = GraphFactory.open(config);
		GraphTraversalSource g = graph.traversal();
		loadData(graph);
		GraphTraversal traversal = getTraversal(g);
		LOG.info("Start traversal ThinkerGraph, vertex count: {}", g.V().count().next());
		return getResult(traversal);
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
			paths.forEach(p -> LOG.info(p.toString()));
		}
	}

	private GraphTraversal getTraversal(GraphTraversalSource g) {
		Function timeAtWarehouse = (Object o) -> {
			Map edgesMap = (Map) o;
			Long readyTime = ((Edge) edgesMap.get("prev")).value("readyTime");
			Long depTime = ((Edge) edgesMap.get("curr")).value("depTime");
			return (depTime - readyTime) >= 0 ? (depTime - readyTime) : Long.MAX_VALUE;
		};

		return g.V().outE("tsw").as("e").inV().emit().repeat(
				__.flatMap(
						__.outE("tsw").filter(__.as("edge").select(last, "e").where(P.eq("edge")).by("speed")).
								group().by(__.inV()).by(__.project("curr", "prev").by().by(__.select(last, "e")).fold()).
								select(Column.values).unfold().order(local).by(timeAtWarehouse).limit(local, 1).select("curr")
				).as("e").inV().simplePath()
		).times(MAX_PATH_LENGTH).map(__.union((Traversal) __.select(last, "e").by("speed"), (Traversal) __.path()).fold());
	}

	private void loadData(Graph graph) {

		Vertex v0 = graph.addVertex("code", "v0");
		Vertex v1 = graph.addVertex("code", "v1");
		Vertex v2 = graph.addVertex("code", "v2");
		Vertex v3 = graph.addVertex("code", "v3");

		v0.addEdge("tsw", v1, "speed", "1", "readyTime", 10L, "depTime", 5L, "dow", 1, "code", "e0");
		v1.addEdge("tsw", v2, "speed", "1", "readyTime", 15L, "depTime", 9L, "dow", 1, "code", "e1"); //must be ignored in longest path
		v1.addEdge("tsw", v2, "speed", "1", "readyTime", 20L, "depTime", 17L, "dow", 1, "code", "e2"); //must be used in longest path
		v2.addEdge("tsw", v3, "speed", "1", "readyTime", 30L, "depTime", 25L, "dow", 1, "code", "e3");
		v1.addEdge("tsw", v2, "speed", "2", "readyTime", 28L, "depTime", 23L, "dow", 1, "code", "e4"); //speed 2
	}

	private Configuration getSqlgConfiguration() {
		Configuration configuration = new BaseConfiguration();
		configuration.addProperty("jdbc.url", JDBC_URL);
		configuration.addProperty("jdbc.username", JDBC_USERNAME);
		configuration.addProperty("jdbc.password", JDBC_PASSWORD);
		return configuration;
	}

	private boolean compareResults(List<SinglePath> etalon, List<SinglePath> other) {
		if (etalon == null || other == null) return false;

		Map<SinglePath, Integer> countEtalon = etalon.stream()
				.distinct()
				.collect(Collectors.toMap(p -> p, p -> Collections.frequency(etalon, p)));
		Map<SinglePath, Integer> countCheck = other.stream()
				.distinct()
				.collect(Collectors.toMap(p -> p, p -> Collections.frequency(other, p)));

		List<SinglePath> theSamePaths = etalon.stream().filter(other::contains).collect(Collectors.toList());

		List<SinglePath> wrongPaths = other.stream()
				.filter(e -> !etalon.contains(e)).collect(Collectors.toList());

		List<SinglePath> missingPaths = etalon.stream()
				.filter(e -> !other.contains(e)).collect(Collectors.toList());

		List<SinglePath> wrongCountPaths = theSamePaths.stream()
				.filter(p -> !countEtalon.get(p).equals(countCheck.get(p))).collect(Collectors.toList());

		List<SinglePath> okPaths = theSamePaths.stream()
				.filter(p -> countEtalon.get(p).equals(countCheck.get(p))).collect(Collectors.toList());

		boolean theSame = Stream.of(
				check(empty, "Wrong Paths", wrongPaths),
				check(empty, "Missing Paths", missingPaths),
				check(empty, "Wrong count Paths", wrongCountPaths),
				check(notEmpty, "Ok Paths", okPaths)).
				allMatch(p -> p);

		if (!theSame) {
			printResult("Ok Paths", okPaths);
		}

		return theSame;
	}

	private boolean check(BiPredicate<String, List<SinglePath>> f, String label, List<SinglePath> list) {
		return f.test(label, list);
	}

	private final BiPredicate<String, List<SinglePath>> empty = (label, list) -> {
		if (!list.isEmpty()) {
			printResult(label, list);
			return false;
		} else {
			return true;
		}
	};

	private final BiPredicate<String, List<SinglePath>> notEmpty = (label, list) -> {
		if (list.isEmpty()) {
			LOG.info("{} is empty", label);
			return false;
		} else {
			return true;
		}
	};

	private class SinglePath {
		private final int speed;
		private final ImmutablePath immutablePath;
		private final String path;

		private SinglePath(Object p) {
			ArrayList<Object> arr = (ArrayList) p;
			this.speed = Integer.valueOf(arr.get(0).toString());
			this.immutablePath = (ImmutablePath) arr.get(1);
			this.path = pathToStr(this.immutablePath);
		}


		private String getPath() {
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
			return "[" + "speed: " + speed +
					", path: " + path +
					']';
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