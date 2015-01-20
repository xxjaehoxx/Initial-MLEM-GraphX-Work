import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// size of the system matrix
val numi = 128*128*360
val numj = 128*128*128
/* number of iterations of MLEM algorithm. you want to change this
depdning on the need*/
val numIterations = 10
/*
number of partitions of the data. you want to change this depending on
the number of nodes you're using*/
val numPartitions = 128
val vLength = numi + numj - 1
val epsilon = 1.0e-13

//importing the values of the elements of system matrix
val edges = sc.textFile("/global/project/projectdirs/paralleldb/sw/nerscpower/SystemMatrix2.csv",numPartitions).map(_.split(',')).map(l => Edge(l(0).toLong+numj, l(1).toLong, l(2).toDouble)).coalesce(numPartitions)

val vertices = sc.parallelize((0 to vLength), numPartitions).map(x => (x.toLong,1.0))

var graph = Graph(vertices, edges).cache()

//importing one projection
val p_measured = sc.textFile("/global/project/projectdirs/paralleldb/sw/nerscpower/MeasuredProjection.csv",numPartitions).map(_.split(',')).map(l => (l(0).toLong+numj, l(1).toDouble)).coalesce(numPartitions)

def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = attr + msgSum

def FPSendMessage(edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = Iterator((edge.srcId, edge.dstAttr * edge.attr))

def BPSendMessage(edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = Iterator((edge.dstId, edge.srcAttr * edge.attr))

def messageCombiner(a: Double, b: Double): Double = a + b

val initialMessage = 0.0
val numIter = 1

graph.vertices.count
graph.edges.count
p_measured.count

/*Pregel is the built-in graph operation modified as a matrix multiplication
operation*/
val start = System.currentTimeMillis
for( i <- 1 to numIterations){
     val CDGraph = graph.mapVertices((vid, attr) => if (vid < numj) 0.0 else attr)
     val const_denom = Pregel(CDGraph, initialMessage, numIter)(vertexProgram, BPSendMessage, messageCombiner).mapVertices((vid, attr) => if (attr < epsilon) 1.0 else 1.0/attr).vertices
     val xFactor = graph.outerJoinVertices(const_denom) { (vid, a, b) => if (vid < numj) a*b.get else a}.vertices
     val FPGraph = graph.mapVertices((vid, attr) => if (vid < numj) attr else 0.0)
     //forward projection (forward matrix multiplication)
     var newGraph = Pregel(FPGraph, initialMessage, numIter)(vertexProgram, FPSendMessage, messageCombiner).cache()
     val p_tmp = newGraph.vertices
     val p_i = newGraph.outerJoinVertices(p_measured) { (vid, a, b) => if (vid < numj) a else {if (a < epsilon) b.get else b.get/a} }
     val BPGraph = p_i.mapVertices((vid, attr) => if (vid < numj) 0.0 else attr)
     //backward projection (backward matrix multiplication)
     newGraph = Pregel(BPGraph, initialMessage, numIter)(vertexProgram, BPSendMessage, messageCombiner).cache()
     graph = newGraph.outerJoinVertices(xFactor) { (vid, a, b) => if (vid < numj) a*b.get else a}.outerJoinVertices(p_tmp) { (vid, a, b) => if (vid < numj) a else b.get}.cache()
     println(graph.vertices.map(_._2).sum())
     }
val end = System.currentTimeMillis
end - start

val answer = graph.vertices.filter{ case (vid, attr) => vid<numj}.filter{ case (vid, attr) => attr>0}
answer.foreach(println)