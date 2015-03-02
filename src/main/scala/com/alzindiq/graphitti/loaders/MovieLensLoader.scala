package com.alzindiq.graphitti.loaders

import java.io.File
import com.alzindiq.graphitti.recommender.MovieLensGraphRecommender
import com.tinkerpop.gremlin.scala.GremlinScala
import com.tinkerpop.gremlin.structure.Graph
import org.apache.log4j.Logger

import scala.io.Source

case class MovieLensLoader( graph : Graph) {
  val gs = GremlinScala (graph)
  def parseFile(file : File, encoding : String = "UTF-8", lineSplitter : String, label: String): Unit = {
    if(MovieLensLoader.LOG.isTraceEnabled)MovieLensLoader.LOG.trace("Parsing File "+file.getName)
    var i =0
    Source.fromFile(file, encoding).getLines().takeWhile(a => i< 10000).foreach( line => {
      if(i % 1000 ==0) println("Step "+i)
      i+=1
      if(MovieLensLoader.LOG.isTraceEnabled)MovieLensLoader.LOG.trace("Parsing line "+line)
      def values = line.split(lineSplitter)
      if (label.equals(MovieLensGraphRecommender.MOVIES)) {
        val movieVertex = gs.addVertex()
        movieVertex.setProperty("label", label)
        movieVertex.setProperty("id", values(0))
        movieVertex.setProperty("title", values(1))
        movieVertex.setProperty("genres",values(2))
      }
      if (label.equals(MovieLensGraphRecommender.USERS)) {
        val userVertex = gs.addVertex()
        userVertex.setProperty("label", label)
        userVertex.setProperty("id", values(0))
        userVertex.setProperty("gender", values(1))
        userVertex.setProperty("age", values(2))
        val occupation = MovieLensLoader.occupations(Integer.valueOf(values(3)))
        def hits = gs.V.has("label", MovieLensGraphRecommender.OCCUPATIONS).filter( v => v.property("id").value.toString.equals(occupation)).headOption
        if (hits.isEmpty) {
          val occVertex = gs.addVertex()
          occVertex.setProperty("label",MovieLensGraphRecommender.OCCUPATIONS)
          occVertex.setProperty("id", occupation)
          userVertex.addEdge("hasOccupation", occVertex, Map())
        } else {
          val occVertex = hits.head
          userVertex.addEdge("hasOccupation", occVertex, Map())
        }
      }
      if (label.equals(MovieLensGraphRecommender.RATINGS)) {
        val userVertex = gs.V.has("label", MovieLensGraphRecommender.USERS).filter( v => v.property("id").value.toString.equals(values(0))).headOption
        val movieVertex = gs.V.has("label", MovieLensGraphRecommender.MOVIES).filter( v => v.property("id").value.toString.equals(values(1))).headOption
        if (!userVertex.isEmpty && !movieVertex.isEmpty) {
          val edge =userVertex.get.addEdge("rated", movieVertex.get, "stars", values(2))
        }
      }
    })
  }
}

object MovieLensLoader{
  val LOG = Logger.getLogger(MovieLensLoader.getClass.getName)
  val occupations = Map(0 -> "other", 1 -> "academic/educator", 2->"artist",
  3->"clerical/admin", 4->"college/grad student", 5->"customer service",
  6->"doctor/health care", 7->"executive/managerial", 8->"farmer",
  9->"homemaker", 10->"K-12 student", 11->"lawyer", 12->"programmer",
  13->"retired", 14->"sales/marketing", 15->"scientist", 16->"self-employed",
  17->"technician/engineer", 18->"tradesman/craftsman", 19->"unemployed", 20->"writer")
}