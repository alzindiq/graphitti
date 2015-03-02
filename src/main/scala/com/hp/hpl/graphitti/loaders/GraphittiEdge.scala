package com.hp.hpl.graphitti.loaders

import com.tinkerpop.gremlin.scala.ScalaVertex

trait GraphittiEdge {
  def id : AnyRef
  def inVtx : ScalaVertex
  def outVtx : ScalaVertex
  def label : String
}

case class Retail2GoEdge(id : Integer, inVtx: ScalaVertex, outVtx: ScalaVertex, label: String)  extends GraphittiEdge