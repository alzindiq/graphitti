package com.hp.hpl.graphitti.loaders

trait GraphittiVertex  {
  def properties : Option[Map[String, Any]]
}

object Retail2GoVertex{
  val CUSTOMER = "customer"
  val PURCHASE = "purchase"
  val PRODUCT = "product"
}

case class Retail2GoVertex (properties : Option[Map[String, String]]) extends GraphittiVertex