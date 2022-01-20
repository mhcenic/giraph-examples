import java.io.IOException;
import org.apache.giraph.Algorithm;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class ClosenessCentrality
  extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

  public static final LongConfOption SOURCE_ID = new LongConfOption(
    "SimpleShortestPathsVertex.sourceId",
    1,
    "The shortest paths id"
  );
  private static final Logger LOG = Logger.getLogger(
    ClosenessCentrality.class
  );

  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  @Override
  public void compute(
    Vertex<LongWritable, VertexData, FloatWritable> vertex,
    Iterable<Message> messages
  )
    throws IOException {
    if (getSuperstep() == 0) {
      closeness = new HashMap<Long, Double>();
      for (long i = START_ID; i < GRAPH_SIZE; i++) {
        if (i == vertex.getId().get()) {
          closeness.put(i, -1.0);
        } else {
          closeness.put(i, Double.MAX_VALUE);
        }
      }
      vertex.setValue(new VertexData(closeness));
      ArrayList<Long> vertexIds = new ArrayList<Long>();
      vertexIds.add(vertex.getId().get());
      Iterable<Edge<LongWritable, FloatWritable>> edges = vertex.getEdges();
      for (Edge<LongWritable, FloatWritable> edge : edges) {
        sendMessage(edge.getTargetVertexId(), new Message(1.0, vertexIds));
      }
    } else {
      vertexIds.clear();
      closeness = vertex.getValue().getCloseness();
      double value = 0;
      ArrayList<Long> exist = new ArrayList<Long>();
      Iterable<Edge<LongWritable, FloatWritable>> edges = vertex.getEdges();
      for (Message message : messages) {
        value = message.getValue();
        for (int i = 0; i < message.getIds().size(); i++) {
          if (
            closeness.get(message.getIds().get(i)) == Double.MAX_VALUE &&
            !exist.contains(message.getIds().get(i))
          ) {
            vertexIds.add(message.getIds().get(i));
            exist.add(message.getIds().get(i));
          }
        }
      }
      if (vertexIds.size() > 0) {
        for (Edge<LongWritable, FloatWritable> edge : edges) {
          sendMessage(
            edge.getTargetVertexId(),
            new Message(value + 1.0, vertexIds)
          );
        }
      }

      for (Message message : messages) {
        for (int i = 0; i < message.getIds().size(); i++) {
          if (closeness.get(message.getIds().get(i)) > message.getValue()) {
            closeness.put(message.getIds().get(i), message.getValue());
          }
        }
      }
      double counter = 0.0;
      for (Map.Entry<Long, Double> entry : closeness.entrySet()) {
        if (
          entry.getValue() != Double.MAX_VALUE &&
          entry.getKey() != vertex.getId().get()
        ) {
          counter = counter + entry.getValue();
        }
      }
      double closeness_value = round((double) ((GRAPH_SIZE - 1) / counter), 2);
      closeness.put(GRAPH_SIZE, closeness_value);
      vertex.setValue(new VertexData(closeness));
      vertex.voteToHalt();
    }
  }
}
