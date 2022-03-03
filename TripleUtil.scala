import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.Map

object TripleUtil {

  // Encoded URIs
  val RDF_TYPE = 0L
  val RDF_PROPERTY = 1L
  val RDF_NIL = 28L
  val RDF_LIST = 27L
  val RDF_FIRST = 26L
  val RDF_REST = 25L
  val RDFS_RANGE = 2L
  val RDFS_DOMAIN = 3L
  val RDFS_SUBPROPERTY = 4L
  val RDFS_SUBCLASS = 5L
  val RDFS_MEMBER = 19L
  val RDFS_LITERAL = 20L
  val RDFS_CONTAINER_MEMBERSHIP_PROPERTY = 21L
  val RDFS_DATATYPE = 22L
  val RDFS_CLASS = 23L
  val RDFS_RESOURCE = 24L
  val OWL_CLASS = 6L
  val OWL_FUNCTIONAL_PROPERTY = 7L
  val OWL_INVERSE_FUNCTIONAL_PROPERTY = 8L
  val OWL_SYMMETRIC_PROPERTY = 9L
  val OWL_TRANSITIVE_PROPERTY = 10L
  val OWL_SAME_AS = 11L
  val OWL_INVERSE_OF = 12L
  val OWL_EQUIVALENT_CLASS = 13L
  val OWL_EQUIVALENT_PROPERTY = 14L
  val OWL_HAS_VALUE = 15L
  val OWL_ON_PROPERTY = 16L
  val OWL_SOME_VALUES_FROM = 17L
  val OWL_ALL_VALUES_FROM = 18L
  val OWL2_PROPERTY_CHAIN_AXIOM = 29

  // Standard URIs
  val S_RDF_NIL = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
  val S_RDF_LIST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#List>"
  val S_RDF_FIRST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>"
  val S_RDF_REST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>"
  val S_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
  val S_RDF_PROPERTY = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>"
  val S_RDFS_RANGE = "<http://www.w3.org/2000/01/rdf-schema#range>"
  val S_RDFS_DOMAIN = "<http://www.w3.org/2000/01/rdf-schema#domain>"
  val S_RDFS_SUBPROPERTY = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>"
  val S_RDFS_SUBCLASS = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
  val S_RDFS_MEMBER = "<http://www.w3.org/2000/01/rdf-schema#member>"
  val S_RDFS_LITERAL = "<http://www.w3.org/2000/01/rdf-schema#Literal>"
  val S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY = "<http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty>"
  val S_RDFS_DATATYPE = "<http://www.w3.org/2000/01/rdf-schema#Datatype>"
  val S_RDFS_CLASS = "<http://www.w3.org/2000/01/rdf-schema#Class>"
  val S_RDFS_RESOURCE = "<http://www.w3.org/2000/01/rdf-schema#Resource>"
  val S_OWL_CLASS = "<http://www.w3.org/2002/07/owl#Class>"
  val S_OWL_FUNCTIONAL_PROPERTY = "<http://www.w3.org/2002/07/owl#FunctionalProperty>"
  val S_OWL_INVERSE_FUNCTIONAL_PROPERTY = "<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>"
  val S_OWL_SYMMETRIC_PROPERTY = "<http://www.w3.org/2002/07/owl#SymmetricProperty>"
  val S_OWL_TRANSITIVE_PROPERTY = "<http://www.w3.org/2002/07/owl#TransitiveProperty>"
  val S_OWL_SAME_AS = "<http://www.w3.org/2002/07/owl#sameAs>"
  val S_OWL_INVERSE_OF = "<http://www.w3.org/2002/07/owl#inverseOf>"
  val S_OWL_EQUIVALENT_CLASS = "<http://www.w3.org/2002/07/owl#equivalentClass>"
  val S_OWL_EQUIVALENT_PROPERTY = "<http://www.w3.org/2002/07/owl#equivalentProperty>"
  val S_OWL_HAS_VALUE = "<http://www.w3.org/2002/07/owl#hasValue>"
  val S_OWL_ON_PROPERTY = "<http://www.w3.org/2002/07/owl#onProperty>"
  val S_OWL_SOME_VALUES_FROM = "<http://www.w3.org/2002/07/owl#someValuesFrom>"
  val S_OWL_ALL_VALUES_FROM = "<http://www.w3.org/2002/07/owl#allValuesFrom>"
  val S_OWL2_PROPERTY_CHAIN_AXIOM = "<http://www.w3.org/2002/07/owl#propertyChainAxiom>"
  val S_OWL2_HAS_KEY = "<http://www.w3.org/2002/07/owl#hasKey>"

  /* Utility High Order Functions */

  def createCombiner = (value: String) => List(value)
  def mergeValue = (list: List[String], value: String) => list ::: (value :: Nil)
  def mergeCombiners = (list1: List[String], list2: List[String]) => list1 ::: list2

  def parseTriple(triple: String) = {
    // Get subject
    val subj = if (triple.startsWith("<")) {
      triple.substring(0, triple.indexOf('>') + 1)
    } else {
      triple.substring(0, triple.indexOf(' '))
    }

    // Get predicate
    val triple0 = triple.substring(triple.indexOf(' ') + 1)
    val pred = triple0.substring(0, triple0.indexOf('>') + 1)

    val triple1 = triple0.substring(pred.length + 1)

    // Get object
    val obj = if (triple1.startsWith("<")) {
      triple1.substring(0, triple1.indexOf('>') + 1)
    }
    else if (triple1.startsWith("\"")) {
      triple1.substring(0, triple1.substring(1).indexOf('\"') + 2)
    }
    else {
      triple1.substring(0, triple1.indexOf(' '))
    }

    (subj, (pred, obj))
  }


  def combination(xs: List[String]) = {
    var ys = List[(String, String)]()
    val n = xs.length
    for (i <- 0 to n - 1; j <- 0 to n - 1) {
      val x = xs(i)
      val y = xs(j)
      if (!x.equals(y)) {
        ys = ys :+ (x, y)
      }
    }
    ys
  }

  def setLogLevel(level: Level): Unit = {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
  }

  // Fixed-poString iteration
  def transitiveClosure(input: RDD[(String, String)], sparkContext: SparkContext) = {

    val hashPartitioner = new HashPartitioner(input.partitions.length)

    var tc = input
    val edges = tc.map { case (s, o) => (o, s) }.partitionBy(hashPartitioner)

    // This join is iterated until a fixed poString is reached.
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      oldCount = nextCount
      tc = tc.union(tc.join(edges).map { case (_, (z, x)) => (x, z) })
        .filter { case (x, y) => !x.equals(y) }.distinct
        .partitionBy(hashPartitioner).cache()
      nextCount = tc.count()


    } while (nextCount != oldCount)

    edges.unpersist(true)
    input.unpersist(true)

    tc
  }

  def transitivePropClosure(input: RDD[(String, (String, String))], sparkContext: SparkContext) = {

    val hashPartitioner = new HashPartitioner(input.partitions.length)

    var tc = input.map { case (s, (p, o)) => ((s, p), o)}
    val edges = tc.map { case ((s, p), o) => ((o, p), s) }.partitionBy(hashPartitioner)

    // This join is iterated until a fixed poString is reached.
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      oldCount = nextCount
      tc = tc.union(tc.join(edges).map { case ((_, p), (s, o)) => ((o, p), s) })
        .filter { case ((o, _), s) => !s.equals(o) }.distinct
        .partitionBy(hashPartitioner).cache()
      nextCount = tc.count()
    } while (nextCount != oldCount)

    edges.unpersist(true)
    input.unpersist(true)

    tc.map { case ((s, p), o) => (s, (p, o)) }
  }

  def combinator(list: List[String]) = {
    for (x <- list; y <- list if !x.equals(y)) yield (x, y)
  }

  def subpropInhReasoning = (bcSubpropMap: Broadcast[Map[String, Iterable[String]]]) =>
    (iterator: Iterator[(String, (String, String))]) => {

      val dict = bcSubpropMap.value
      for {
        (s, (p, o)) <- iterator
        if dict.contains(p)
        q <- dict.get(p).get
      } yield (s, (q, o))
    }

  def filterTranProp = (bcTransPropArray: Broadcast[Array[String]]) =>
    (iterator: Iterator[(String, (String, String))]) => {

      val dict = bcTransPropArray.value
      for {
        (s, (p, o)) <- iterator
        if dict.contains(p)
      } yield (s, (p, o))
    }

  def inverseReasoning1 = (bcInverseOfMap1: Broadcast[Map[String, Iterable[String]]]) =>
    (iterator: Iterator[(String, (String, String))]) => {

      val vp1 = bcInverseOfMap1.value
      for {
        (v, (p, w)) <- iterator
        if (vp1.contains(p))
        q <- vp1.get(p).get
      } yield (w, (q, v))
    }

  def inverseReasoning2 = (bcInverseOfMap2: Broadcast[Map[String, Iterable[String]]]) =>
    (iterator: Iterator[(String, (String, String))]) => {

      val inverseOfMap = bcInverseOfMap2.value
      for {
        (v, (p, w)) <- iterator
        if (inverseOfMap.contains(p))
        q <- inverseOfMap.get(p).get
      } yield (w, (q, v))
    }

  def symmetricReasoning = (bcSymmetricPropMap: Broadcast[Array[String]]) =>
    (iterator: Iterator[(String, (String, String))]) => {

      val symmetricMap = bcSymmetricPropMap.value
      for {
        (v, (p, w)) <- iterator
        if (symmetricMap.contains(p))
      } yield (w, (p, v))
    }

  def hasValueReasoning1 = (joinedHasValOnPropMap1: Broadcast[Map[(String, String), Iterable[String]]]) =>
    (iterator: Iterator[(String, (String, String))]) => {

      val dict = joinedHasValOnPropMap1.value
      for {
        (u, pw) <- iterator
        if dict.contains(pw)
        v <- dict.get(pw).get
      } yield (u, v)
    }

  def hasValueReasoning2 = (joinedHasValOnPropMap2: Broadcast[Map[String, scala.Iterable[(String, String)]]]) =>
    (iterator: Iterator[(String, String)]) => {
      val map = joinedHasValOnPropMap2.value
      for {
        (u, v) <- iterator
        if map.contains(v)
        pw <- map.get(v).get
      } yield (u, pw)
    }

  def filterSomValOnPropTriple = (joinedSomValOnPropMap: Broadcast[Map[(String, String), Iterable[String]]]) =>
    (iterator: Iterator[(String, (String, String))]) => {
      val list = joinedSomValOnPropMap.value.map { case ((_, p), _) => p }.toList
      for {
        (u, (p, x)) <- iterator
        if list.contains(p)
      } yield (x, (p, u))
    }

  def filterSomValOnPropType = (joinedSomValOnPropMap: Broadcast[Map[(String, String), Iterable[String]]]) =>
    (iterator: Iterator[(String, String)]) => {
      val list = joinedSomValOnPropMap.value.map { case ((w, _), _) => w }.toList
      for {
        (x, w) <- iterator
        if list.contains(w)
      } yield (x, w)
    }

  def someValueReasoning = (joinedSomValOnPropMap: Broadcast[Map[(String, String), Iterable[String]]]) =>
    (iterator: Iterator[(String, ((String, String), String))]) => {
      val dict = joinedSomValOnPropMap.value

      for {
        (_, ((p, u), w)) <- iterator
        if dict.contains((w, p))
        v <- dict.get((w, p)).get
      } yield (u, v)
    }

  def filterAllValOnPropTriple = (joinedAllValOnPropMap: Broadcast[Map[(String, String), Iterable[String]]]) =>
    (iterator: Iterator[(String, (String, String))]) => {
      val list = joinedAllValOnPropMap.value.map { case ((_, p), _) => p }.toList

      for {
        (u, (p, x)) <- iterator
        if list.contains(p)
      } yield (x, (p, u))
    }


  def filterAllValOnPropType = (joinedAllValOnPropMap: Broadcast[Map[(String, String), Iterable[String]]]) =>
    (iterator: Iterator[(String, String)]) => {
      val list = joinedAllValOnPropMap.value.map { case ((v, _), _) => v }.toList

      for {
        (u, v) <- iterator
        if list.contains(v)
      } yield (u, v)
    }


  def allValueReasoning = (joinedAllValOnPropMap: Broadcast[Map[(String, String), Iterable[String]]]) =>
    (iterator: Iterator[(String, ((String, String), String))]) => {
      val dict = joinedAllValOnPropMap.value

      for {
        (_, ((p, x), v)) <- iterator
        if dict.contains((v, p))
        w <- dict.get((v, p)).get
      } yield (x, w)
    }

  def subclassReasoning = (bcSubclassMap: Broadcast[Map[String, Iterable[String]]]) =>
    (iterator: Iterator[(String, String)]) => {
      val subClassMap = bcSubclassMap.value
      for {
        (u, c) <- iterator
        if subClassMap.contains(c)
        c1 <- subClassMap.get(c).get
      } yield (u, c1)
    }

  def domainReasoning = (bcDomainMap: Broadcast[Map[String, Iterable[String]]]) =>
    (iterator: Iterator[(String, (String, String))]) => {
      val domDict = bcDomainMap.value
      for {
        (s, (p, o)) <- iterator
        if (domDict.contains(p))
        q <- domDict.get(p).get
      } yield (s, q)
    }

  def rangeReasoning = (bcRangeMap: Broadcast[Map[String, Iterable[String]]]) =>
    (iterator: Iterator[(String, (String, String))]) => {
      val rangeMap = bcRangeMap.value
      for {
        (s, (p, o)) <- iterator
        if (rangeMap.contains(p))
        q <- rangeMap.get(p).get
      } yield (o, q)
    }

  def filterInvFuncProp = (bcFuncProp: Broadcast[Array[String]]) =>
    (iterator: Iterator[(String, (String, String))]) => {
      val funcPropMap = bcFuncProp.value
      for {
        (s, (p, o)) <- iterator
        if funcPropMap.contains(p)
      } yield ((p, o), s)
    }

  def filterFuncProp = (bcInvFuncProp: Broadcast[Array[String]]) =>
    (iterator: Iterator[(String, (String, String))]) => {
      val invFuncPropMap = bcInvFuncProp.value
      for {
        (s, (p, o)) <- iterator
        if invFuncPropMap.contains(p)
      } yield ((s, p), o)
    }

}
