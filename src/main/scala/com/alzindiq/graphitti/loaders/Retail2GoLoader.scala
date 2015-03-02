package com.alzindiq.graphitti.loaders

import java.io.File

import com.tinkerpop.gremlin.scala.{ScalaGraph, GremlinScala}
import com.tinkerpop.gremlin.structure.Graph
import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ListBuffer

case class Retail2GoLoader(file : File, encoding : String = "UTF-8", graph : Graph)
  extends GraphXSVLoader (file, encoding, graph) {

  var prodCount = 0
  var edgeCount = 0
  if (Option(file).isEmpty) throw new NoFileError("Could not find file object!")

  def parseVertices(line: String, splitter : String): List[GraphittiVertex] = {
    val values = line.split(splitter)
    var vertices: ListBuffer[GraphittiVertex] = new ListBuffer()
    if (validateLine(line, values)) {
      val gs = GremlinScala(graph)
      // customer, purchase, product
      if (!containsVertex(values(0),Retail2GoVertex.CUSTOMER,gs)) {
        vertices += new Retail2GoVertex(Some(Map(
          "id" -> values(0),
          "label" -> Retail2GoVertex.CUSTOMER
        )))
      }
      // hope a real dataset comes with a product index!!
      val product = new Retail2GoVertex(Some(Map(
        "id" -> String.valueOf(prodCount),
        "label" -> Retail2GoVertex.PRODUCT,
        "chain" -> values(1),
        "dept" -> values(2),
        "category" -> values(3),
        "company" -> values(4),
        "brand" -> values(5),
        "productsize" -> values(7),
        "productmeasure" -> values(8),
        "eveniser" -> "even"
      )))
      val existingProduct = gs.V.has("label", Retail2GoVertex.PRODUCT).has("chain", values(1)).has("dept", values(2)).has("category", values(3))
        .has("company", values(4)).has("brand", values(5)).has("productsize", values(7)).has("productmeasure", values(8)).headOption

      if (existingProduct.isEmpty) {
        vertices += product
        prodCount += 1
      }
    }
    vertices.toList
  }

  def parseEdges(line: String, splitter : String): List[GraphittiEdge] = {
    val values = line.split(splitter)
    var edges: ListBuffer[GraphittiEdge] = new ListBuffer()
    if (validateLine(line, values)) {
      val gs = GremlinScala(graph)
      // customer, purchase, product
      val customer = gs.V.has("label", Retail2GoVertex.CUSTOMER).has("id", values(0)).headOption
      if (customer.isEmpty) {
        edges.toList
      } else {
        // hope a real dataset comes with a product index!!
        val existingProduct = gs.V.has("label", Retail2GoVertex.PRODUCT).has("chain", values(1)).has("dept", values(2)).has("category", values(3))
          .has("company", values(4)).has("brand", values(5)).has("productsize", values(7)).has("productmeasure", values(8)).headOption

        if (existingProduct.isEmpty) {
          throw new NonExistentVertexException("This line should have been preprocessed and the vertex should exist. \n" +
            "Did you run loadVertices before? \n" +
            "Any concurrent modification while batch loading the file?")
        } else {
          val in = new Retail2GoEdge(edgeCount, customer.get, existingProduct.get, Retail2GoLoader.CUST2PROD)
          edges += in
          edgeCount += 1
          val out = new Retail2GoEdge(edgeCount, existingProduct.get, customer.get, Retail2GoLoader.PROD2CUST)
          edges += out
          edgeCount += 1
        }
        edges.toList
      }
    }
    edges.toList
  }

  def validateLine(line: String, values: Array[String]): Boolean = {
    if(line.isEmpty){
      false
    }else{
      if (!values.size.equals(11)) {
        Retail2GoLoader.LOG.warn("Line: " + line + " does not have 11 elements: Wrong length")
        false
      }else {
        val allNotEmpty = values.map(value => !Option(value).isEmpty).foldLeft(true)((a, b) => a && b)
        if (!allNotEmpty) {
          Retail2GoLoader.LOG.warn("Line: " + line + " contains some empty elements")
          false
        }else {
          val wellFormatted: Boolean = values.map(a => {
            isNumeric(a) || isDate(a) || isValidUnit(a)
          }).foldLeft(true)((a, b) => a && b)
          if (!wellFormatted) {
            Retail2GoLoader.LOG.warn("Line: " + line + " contains elements with a wrong format")
            false
          }else{
            true
          }
        }
      }
    }
  }

  def isValidUnit (str:String): Boolean = {
    Retail2GoLoader.validUnits.contains(str)
  }

  def isNumeric(str: String): Boolean = {
    !throwsNumberFormatException(str.toLong) || !throwsNumberFormatException(str.toDouble)
  }

  def throwsNumberFormatException(f: => Any): Boolean = {
    try { f; false } catch { case e: NumberFormatException => true }
  }

  def isDate(input: String): Boolean = try {
    Retail2GoLoader.fmt parseDateTime input
    true
  } catch {
    case e: IllegalArgumentException => false
  }

  def containsVertex(id: String, label:String, gs: ScalaGraph): Boolean = try {
    gs.V.has("label",label).has("id",id).head
    true
  }catch {
    case e: NoSuchElementException => false
  }
  // O(N) load process, but two passes are required (2N)
  this.loadVertices(Retail2GoLoader.lineToSkip,",")
  this.loadEdges(Retail2GoLoader.lineToSkip,",")
}

object Retail2GoLoader {
  val LOG = Logger.getLogger(Retail2GoLoader.getClass.getName)
  val fmt = DateTimeFormat forPattern "yyyy-MM-dd"
  val lineToSkip = "id,chain,dept,category,company,brand,date,productsize,productmeasure,purchasequantity,purchaseamount"
  val validUnits = List("OZ","M","CM")
  val CUST2PROD = "bought"
  val PROD2CUST = "has bought"
}
