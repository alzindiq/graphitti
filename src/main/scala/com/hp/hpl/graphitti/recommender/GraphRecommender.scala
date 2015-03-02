package com.hp.hpl.graphitti.recommender

import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph
import com.tinkerpop.gremlin.scala.{ScalaVertex,ScalaGraph}
import com.tinkerpop.gremlin.structure.{Edge, Graph, Vertex}
import org.apache.log4j.Logger


trait GraphRecommender {
  def getRecommendation(targetVertex : ScalaVertex, topN : Integer) : List[String]
  def getRecommendation(targetVertexId : String, label : String, topN : Integer) : List[String]
}

case class MovieLensGraphRecommender(edgeQualityFilter : Edge => Boolean, contentFilter : Vertex => Boolean, userSimilarityFilter : Vertex => Boolean)
                                    (graph : Graph) extends GraphRecommender{

  def this(edgeQualityFilter : Edge => Boolean, contentFilter : Vertex => Boolean, userSimilarityFilter : Vertex => Boolean,dbPath : String)=  {
    this(edgeQualityFilter,contentFilter,userSimilarityFilter)(Neo4jGraph.open(dbPath))
  }


  val gs = ScalaGraph(graph)

  def getRecommendation(targetVertexId : String, label : String, topN : Integer) : List[String] = {
    val targetVertex=gs.V.has("label", label).has("id",targetVertexId).headOption()
    if(targetVertex.isEmpty){
      if(MovieLensGraphRecommender.LOG.isTraceEnabled) MovieLensGraphRecommender.LOG.trace("provided a nonexistent vertex:  "+targetVertexId)
      List.empty[String]
    }else{
      getRecommendation(targetVertex.get, topN)
    }
  }

  override def getRecommendation(user: ScalaVertex, topN : Integer): List[String] = {
    if(!user.property("label").value.toString.equals(MovieLensGraphRecommender.USERS)){
      if(MovieLensGraphRecommender.LOG.isTraceEnabled) MovieLensGraphRecommender.LOG.trace("Provided a vertex of the wrong type. It should be a "+MovieLensGraphRecommender.USERS)
      List.empty[String]
    }else{
      val userTopRatedMovies = user.outE("rated").filter(edgeQualityFilter).inV.toList
      if(MovieLensGraphRecommender.LOG.isDebugEnabled) MovieLensGraphRecommender.LOG.debug("User top rated: "+ userTopRatedMovies.map(v => MovieLensGraphRecommender.getPropertyValueAsString(v,"id")))
      val allRecs=user.outE("rated").filter(edgeQualityFilter).inV
        .inE("rated").outV.filter(v => v != user).filter(userSimilarityFilter)
        .outE("rated").filter(edgeQualityFilter).inV.except(userTopRatedMovies).filter(contentFilter).toList

      val recMap : scala.collection.mutable.Map[String,Int] =getCountMap(allRecs)
      if(MovieLensGraphRecommender.LOG.isDebugEnabled) MovieLensGraphRecommender.LOG.debug("Unordered map: "+recMap)
      recMap.toSeq.sortWith(_._2 > _._2).toMap.keySet.take(topN).toList
    }
  }

  def getCountMap(vertices: List[Vertex]): scala.collection.mutable.Map[String, Int] = {
    val tuples = vertices.map(v => (MovieLensGraphRecommender.getPropertyValueAsString(v,"id"), 1)).toList
    var count : scala.collection.mutable.Map[String,Int]= scala.collection.mutable.Map[String,Int]()
    for((k,v)<- tuples){
      val value= count.getOrElse(k,0)+1
      count(k)=value
    }
    count
  }
}

object MovieLensGraphRecommender {
  val LOG = Logger.getLogger(MovieLensGraphRecommender.getClass.getName)
  val USERS = "User"
  val RATINGS = "Rating"
  val MOVIES = "Movie"
  val GENRES = "Genre"
  val OCCUPATIONS = "Occupation"

  def getPropertyValueAsString(v: Vertex, s: String): String ={
    v.property(s).value.toString
  }
}