package com.alzindiq.graphitti.loaders

import java.io.File

import com.tinkerpop.gremlin.scala.{GremlinScala, ScalaVertex}
import com.tinkerpop.gremlin.structure.Graph
import org.apache.log4j.Logger

import scala.io.Source

object GraphXSVLoader {
  val LOG = Logger.getLogger(GraphXSVLoader.getClass.getName)
}

abstract class GraphXSVLoader (file : File, encoding : String = "UTF-8", graph : Graph) {
  protected def loadVertices (line2Skip : String, lineSplitter : String)
  {
    val gs = GremlinScala(graph)
    Source.fromFile(file, encoding).getLines().foreach( line => {
      val strLine = line.toString.trim
      if (!strLine.equals(line2Skip)) {
        val toBeAdded: List[GraphittiVertex] = parseVertices(strLine, lineSplitter)
        toBeAdded.foreach(vtx => {
          if(GraphXSVLoader.LOG.isTraceEnabled) GraphXSVLoader.LOG.trace("Loading vtx: "+vtx)
          val x : ScalaVertex =gs.addVertex()
          for( (k,v) <- vtx.properties.get) x.setProperty(k,v)

        })
      }
    })
  }

  protected def loadEdges (line2Skip : String, lineSplitter : String)
  {
    val gs = GremlinScala(graph)
    Source.fromFile(file, encoding).getLines().foreach( line => {
      val strLine = line.toString.trim
      if (!strLine.equals(line2Skip)) {
        val toBeAdded: List[GraphittiEdge] = parseEdges(strLine, lineSplitter)
        toBeAdded.foreach(e => {
          if(GraphXSVLoader.LOG.isTraceEnabled) GraphXSVLoader.LOG.trace("E  "+e)
          e.inVtx.addEdge(e.label,  e.outVtx, Map())
        })
      }
    })
  }

  def parseVertices(line : String, lineSplitter : String) : List[GraphittiVertex]
  def parseEdges(line: String, lineSplitter : String): List[GraphittiEdge]
}

trait GraphLoaderError extends Throwable{
  def msg: String
}
case class NoFileError (override val msg: String) extends GraphLoaderError
case class WrongXSVLine (override val msg: String) extends GraphLoaderError
case class NonExistentVertexException (override val msg: String) extends GraphLoaderError
