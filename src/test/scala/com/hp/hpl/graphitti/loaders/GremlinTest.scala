package com.hp.hpl.graphitti.loaders

import java.io.File
import java.nio.file.{Files, Paths}

import com.hp.hpl.graphitti.recommender.MovieLensGraphRecommender
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph
import com.tinkerpop.gremlin.scala.GremlinScala
import com.tinkerpop.gremlin.structure.{Edge, Vertex}
import org.apache.commons.io.FileUtils
import org.neo4j.graphdb.{Transaction, GraphDatabaseService, DynamicLabel}
import org.scalatest.{Matchers, FlatSpec}

class GraphMovieLoaderTest extends FlatSpec with Matchers {


  "GraphLoader" should "load data set" in {
    val dbPath = "db/movieLens"
    val dbFile = new File(dbPath)
    if(Files.exists(Paths.get(dbPath)))
      FileUtils.cleanDirectory(dbFile)
    FileUtils.forceMkdir(dbFile)

    val graph: Neo4jGraph = Neo4jGraph.open(dbPath)

    graph.tx.open
    val l = DynamicLabel.label(MovieLensGraphRecommender.MOVIES)
    val s = graph.getBaseGraph.schema
    s.indexFor(l).on("id").create

    val lab = DynamicLabel.label(MovieLensGraphRecommender.USERS)
    s.indexFor(lab).on("id").create
    graph.tx.close

    val movieLoader = new MovieLensLoader(graph)

    graph.tx.open
    println("Loading movies")
    //val movieFile = new File("resources/ml-1m/movies.dat")
    val movieFile = new File("resources/all/movies.dat")
    movieLoader.parseFile(movieFile,"ISO-8859-1","::",MovieLensGraphRecommender.MOVIES)

    println("Loading users")
    //val userFile = new File("resources/ml-1m/users.dat")
    val userFile = new File("resources/all/users.dat")
    movieLoader.parseFile(userFile,"ISO-8859-1","::",MovieLensGraphRecommender.USERS)

    println("Loading ratings")
    //val ratingFile = new File("resources/ml-1m/ratings.dat")
    val ratingFile = new File("resources/all/ratings.dat")
    movieLoader.parseFile(ratingFile,"ISO-8859-1","::",MovieLensGraphRecommender.RATINGS)

    graph.tx.commit
    graph.tx.close


    val gs = GremlinScala(graph)
    gs.V.toList.length shouldBe 28
    gs.E.toList.length shouldBe 28
    gs.V.has("label",MovieLensGraphRecommender.MOVIES).toList.length shouldBe 20
    gs.V.has("label",MovieLensGraphRecommender.USERS).toList.length shouldBe 4
    gs.V.has("label",MovieLensGraphRecommender.OCCUPATIONS).toList.length shouldBe 4
    val starredEdges=gs.E.has("stars").toList
    starredEdges.map(e => Integer.valueOf(e.property("stars").value.toString) ).toList.foldRight(0)((a,b) => a+b) shouldBe 82

    val min4StarEdgeThreshold = (e:Edge) => Integer.valueOf(e.property("stars").value().toString) > 3
    val min3StarEdgeThreshold = (e:Edge) => Integer.valueOf(e.property("stars").value().toString) > 2
    val min1StarEdgeThreshold = (e:Edge) => Integer.valueOf(e.property("stars").value().toString) > 0

    val bannedGenres = Set("Animation","Comedy")
    val bannedGenreEvaluator = (v:Vertex) => v.property("genres").value.toString.split("\\|").toList
      .map(genre => !bannedGenres.contains(genre)).foldRight(true)(_ && _)

    val noMaleUnder26 = (v:Vertex) => Integer.valueOf(v.property("age").value.toString) > 26 &&
                                                      v.property("gender").value.toString.equals("F")

    val doNotFilterUsers= (v:Vertex) => true
    val doNotBanGenres = (v:Vertex) => true
    val doNotFilterProductEdges = (e:Edge) => true

    println("Testing for 10 similar movies with 3+ starts in the neighbourhood of user 2")
    val all3StarsRec = MovieLensGraphRecommender(min3StarEdgeThreshold,doNotBanGenres,doNotFilterUsers)(graph)
    val all3Stars = all3StarsRec.getRecommendation("2",MovieLensGraphRecommender.USERS,10)
    println("Checking right number of results")
    all3Stars.size shouldBe 7
    val all3 = List("8", "9", "5", "6", "20", "2", "18")
    println("Checking correct response")
    all3Stars.map(s => all3.contains(s)).foldRight(true)(_ && _) shouldBe true
    println("Checking right order")
    all3Stars.head shouldBe "8"
    all3Stars.last shouldBe "18"

    println("Testing for 10 similar movies with 3+ starts in the neighbourhood of user 2, excluding "+bannedGenres)
    val noAnimationNoComedy3StarsRec = MovieLensGraphRecommender(min3StarEdgeThreshold,bannedGenreEvaluator,doNotFilterUsers)(graph)
    val noAnimationNoComedy3Stars = noAnimationNoComedy3StarsRec.getRecommendation("2",MovieLensGraphRecommender.USERS,10)
    noAnimationNoComedy3Stars.size shouldBe 5
    val banned3Star = List("9", "6", "20", "2", "18")
    println("Checking correct response")
    noAnimationNoComedy3Stars.map(s => banned3Star.contains(s)).foldRight(true)(_ && _) shouldBe true

    println("Testing for 10 similar movies with 3+ starts in the neighbourhood of user 2, excluding "+bannedGenres+" and filtering males under 26")
    val noAnimationNoComedyNoMaleUnder263StarsRec = MovieLensGraphRecommender(min3StarEdgeThreshold,bannedGenreEvaluator,noMaleUnder26)(graph)
    val noAnimationNoComedyNoMaleUnder263Stars = noAnimationNoComedyNoMaleUnder263StarsRec.getRecommendation("2",MovieLensGraphRecommender.USERS,10)
    noAnimationNoComedyNoMaleUnder263Stars.size shouldBe 3
    val filteredBanned3Star = List("9", "6", "18")
    println("Checking correct response")
    noAnimationNoComedyNoMaleUnder263Stars.map(s => filteredBanned3Star.contains(s)).foldRight(true)(_ && _) shouldBe true

    println("Testing for 10 similar movies with 3+ starts in the neighbourhood of user 2, filtering males under 26")
    val noMaleUnder263StarsRec = MovieLensGraphRecommender(min3StarEdgeThreshold,doNotBanGenres,noMaleUnder26)(graph)
    val noMaleUnder263Stars = noMaleUnder263StarsRec.getRecommendation("2",MovieLensGraphRecommender.USERS,10)
    noMaleUnder263Stars.size shouldBe 4
    val filtered3Star = List("8", "18", "6", "9")
    noMaleUnder263Stars.map(s => filtered3Star.contains(s)).foldLeft(true)(_ && _)

    println("Testing for 10 similar movies with 1+ starts in the neighbourhood of user 2")
    val all1StarsRec = MovieLensGraphRecommender(min1StarEdgeThreshold,doNotBanGenres,doNotFilterUsers)(graph)
    val all1Stars = all1StarsRec.getRecommendation("2",MovieLensGraphRecommender.USERS,10)
    println("Checking right number of results ")
    all1Stars.size shouldBe 10
    val all1 = List("12", "8", "4", "9", "5", "10", "6", "20", "2", "18")
    all1Stars.map(s => all1.contains(s)).foldLeft(true)(_ && _)
    all1Stars.head shouldBe "12"
    all1Stars.last shouldBe "18"

    println("Testing for 10 similar movies with 1+ starts in the neighbourhood of user 2, limited to 5 results")
    val all1StarsLimitedRec = MovieLensGraphRecommender(min1StarEdgeThreshold,doNotBanGenres,doNotFilterUsers)(graph)
    val all1StarsLimited = all1StarsLimitedRec.getRecommendation("2",MovieLensGraphRecommender.USERS,5)
    println("Checking right number of results ")
    all1StarsLimited.size shouldBe 5
    val all1Limited = List("12", "8", "4", "9", "5")
    all1StarsLimited.map(s => all1Limited.contains(s)).foldLeft(true)(_ && _)
    all1StarsLimited.head shouldBe "12"
    all1StarsLimited.last shouldBe "5"

    println("Pretend users inject a vertex of a wrong type")
    val wrongTypeRec = MovieLensGraphRecommender(min1StarEdgeThreshold,doNotBanGenres,doNotFilterUsers)(graph)
    val wrongType = wrongTypeRec.getRecommendation("18",MovieLensGraphRecommender.USERS,5)
    println("Checking right number of results ")
    wrongType.size shouldBe 0

    println("Non-existent vertex")
    val nonExistRec = MovieLensGraphRecommender(min1StarEdgeThreshold,doNotBanGenres,doNotFilterUsers)(graph)
    val nonExist = nonExistRec.getRecommendation("98",MovieLensGraphRecommender.USERS,5)
    println("Checking right number of results ")
    wrongType.size shouldBe 0

    graph.close()
  }
}

class GraphWrongLoadingTest extends FlatSpec with Matchers {

  "GraphLoader" should "recover from failure derived from parsing crappy data" in {
    val csvFile = new File("testCrappy.txt")
    val dbPath = "db/retail2Go"
    val dbFile = new File(dbPath)
    if(Files.exists(Paths.get(dbPath)))
      FileUtils.cleanDirectory(dbFile)
    FileUtils.forceMkdir(dbFile)
    val graph: Neo4jGraph = Neo4jGraph.open(dbPath)

    graph.tx.open
    val l = DynamicLabel.label(Retail2GoVertex.CUSTOMER)
    val s = graph.getBaseGraph.schema
    s.indexFor(l).on("id").create

    val lab = DynamicLabel.label(Retail2GoVertex.PRODUCT)
    s.indexFor(lab).on("id").create
    graph.tx.close

    val graphLoader = new Retail2GoLoader(csvFile,"UTF-8",graph)

    val gs = GremlinScala(graph)
    gs.V.toList.length shouldBe 4
    gs.E.toList.length shouldBe 4
    gs.V.has("label",Retail2GoVertex.CUSTOMER).toList.length shouldBe 2
    gs.V.has("label",Retail2GoVertex.PRODUCT).toList.length  shouldBe 2
    // check edges exist
    gs.E.filter( e => e.label().equals(Retail2GoLoader.PROD2CUST)).toList.size shouldBe 2
    gs.E.filter( e => e.label().equals(Retail2GoLoader.CUST2PROD)).toList.size shouldBe 2
  }
}

class GraphLoadingTest extends FlatSpec with Matchers {

    "GraphLoader" should "create some vertices with right edges" in {
      val csvFile = new File("test.txt")
      val dbPath = "db/retail2Go"
      val dbFile = new File(dbPath)
      if(Files.exists(Paths.get(dbPath)))
        FileUtils.cleanDirectory(dbFile)
      FileUtils.forceMkdir(dbFile)
      val graph: Neo4jGraph = Neo4jGraph.open(dbPath)

      graph.tx.open
      val l = DynamicLabel.label(Retail2GoVertex.CUSTOMER)
      val s = graph.getBaseGraph.schema
      s.indexFor(l).on("id").create

      val lab = DynamicLabel.label(Retail2GoVertex.PRODUCT)
      s.indexFor(lab).on("id").create
      graph.tx.close

      val graphLoader = new Retail2GoLoader(csvFile,"UTF-8",graph)

      val gs = GremlinScala(graph)
      gs.V.toList.length shouldBe 3
      gs.E.toList.length shouldBe 4
      gs.V.has("label",Retail2GoVertex.CUSTOMER).toList.length shouldBe 2
      gs.V.has("label",Retail2GoVertex.PRODUCT).toList.length  shouldBe 1
      // check edges exist
      gs.V.has("label",Retail2GoVertex.PRODUCT).head.out(Retail2GoLoader.PROD2CUST).toList.size shouldBe 2
      gs.V.has("label",Retail2GoVertex.CUSTOMER).out(Retail2GoLoader.CUST2PROD).toSet.size shouldBe 1
    }
}


