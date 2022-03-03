
import TripleUtil._
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel 
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.Map

object OWLReasoner {

  def main(args: Array[String]): Unit = {

    setLogLevel(Level.WARN)

    require (args.length == 2, "Requires 2 arguments: arg1 -> input folder, arg2 -> output folder")

    val inputFile = args(0)

    val conf = new SparkConf()
      .setAppName("OWL Horst Inference")//.setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val startTime = System.currentTimeMillis()
    val emptyArray: Array[String] = Array()
    val emptyRDD: RDD[String] = sparkContext.parallelize(emptyArray)

    try {
      val tripleRDD = sparkContext
        .textFile(inputFile)
        .map(parseTriple)
        .filter { case (_, (p, _)) => !p.equals(TripleUtil.S_RDF_TYPE) || !p.equals(TripleUtil.S_OWL_SAME_AS) }
        .persist(StorageLevel.MEMORY_ONLY)
      tripleRDD.count()

      val slices = tripleRDD.mapPartitions(iter=>{Iterator(1)}, true).reduce((x,y)=> x + y)

      val partitioner = new HashPartitioner(slices)

      println("Number of input triples : " + tripleRDD.count())

      var equivClassRDD = tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_OWL_EQUIVALENT_CLASS) }
        .map { case (s, (_, o)) => (s, o) }
        .distinct()
        .repartition(slices / 10)
        .persist(StorageLevel.MEMORY_ONLY_SER)

      var equivPropRDD = tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_OWL_EQUIVALENT_PROPERTY) }
        .map { case (s, (_, o)) => (s, o) }
        .distinct()
        .repartition(slices / 10)
        .cache()

      var subclassRDD = tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_RDFS_SUBCLASS) }
        .map { case (s, (_, o)) => (s, o) }
        .distinct()
        .repartition(slices / 10)
        .cache()

      var subpropRDD = tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_RDFS_SUBPROPERTY) }
        .map { case (s, (_, o)) => (s, o) }
        .distinct()
        .repartition(slices / 10)
        .cache()

      /* SCHEMA REASONING */

      /* Subclass transitive and equivalent class reasoning */

      val subclassCount = subclassRDD.count()

      subclassRDD = subclassRDD.union(equivClassRDD)
      subclassRDD = equivClassRDD
        .map { case (s, o) => (o, s) }
        .union(subclassRDD)
        .distinct()
        .repartition((slices / 10))

      subclassRDD = transitiveClosure(subclassRDD, sparkContext)

      println("Number of new subclasses : " + (subclassRDD.count() - subclassCount))

      val newEquivClassRDD = subclassRDD
        .join(subclassRDD.map { case (w, v) => (v, w) })
        .filter { case (v, (w1, w2)) => w1.equals(w2) && !v.equals(w1) }
        .map { case (v, (w, _)) => (v, w) }
        .subtract(equivClassRDD)

      println("Number of new equivalent classes : " + newEquivClassRDD.count())

      val bcSubclassMap = sparkContext.broadcast(subclassRDD.groupByKey().collectAsMap())

      equivClassRDD = equivClassRDD.union(newEquivClassRDD).distinct()
      equivClassRDD.count()

      /* Subproperty transitive and equivalent property reasoning */

      val subpropCount = subpropRDD.count()

      subpropRDD = subpropRDD.union(equivPropRDD)
      subpropRDD = equivPropRDD
        .map { case (s, o) => (o, s) }
        .union(subpropRDD)
        .distinct()
        .repartition(slices / 10)

      subpropRDD = transitiveClosure(subpropRDD, sparkContext)

      println("Number of new subproperties : " + (subpropRDD.count() - subpropCount))

      val newEquivProps = subpropRDD
        .join(subpropRDD.map { case (w, v) => (v, w) })
        .filter { case (v, (w1, w2)) => w1.equals(w2) && !v.equals(w1) }
        .map { case (v, (w, _)) => (v, w) }
        .subtract(equivPropRDD)

      println("Number of new equivalent properties : " + newEquivProps.count())

      equivPropRDD = equivPropRDD.union(newEquivProps).distinct()
      equivPropRDD.count()

      /* Broadcast schema data */

      val bcSubpropMap = sparkContext.broadcast(subpropRDD.groupByKey.collectAsMap())
      subpropRDD.unpersist(true)

      val bcInverseOfMap1 = sparkContext.broadcast {
        tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_OWL_INVERSE_OF) }
          .map { case (s, (_, o)) => (s, o) }
          .distinct()
          .groupByKey()
          .collectAsMap()
      }

      val bcInverseOfMap2 = sparkContext.broadcast {
        tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_OWL_INVERSE_OF) }
          .map { case (s, (_, o)) => (o, s) }
          .distinct()
          .groupByKey()
          .collectAsMap()
      }

      val bcSymmetricPropMap = sparkContext.broadcast {
        tripleRDD.filter { case (_, (p, o)) => p.equals(TripleUtil.S_RDF_TYPE) && o.equals(TripleUtil.S_OWL_SYMMETRIC_PROPERTY) }
          .map { case (s, _) => s }
          .distinct()
          .collect()
      }

      val bcTransPropArray = sparkContext.broadcast(
        tripleRDD.filter { case (_, (p, o)) => p.equals(TripleUtil.S_RDF_TYPE) && o.equals(TripleUtil.S_OWL_TRANSITIVE_PROPERTY) }
          .map { case (s, _) => s }.collect()
      )

      val bcDomainMap = sparkContext.broadcast {
        tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_RDFS_DOMAIN) }
          .map { case (s, (_, o)) => (s, o) }
          .distinct()
          .groupByKey()
          .collectAsMap()
      }

      val bcRangeMap = sparkContext.broadcast {
        tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_RDFS_RANGE) }
          .map { case (s, (_, o)) => (s, o) }
          .distinct()
          .groupByKey()
          .collectAsMap()
      }

      val onPropRDD = tripleRDD
        .filter { case (_, (p, _)) => p.equals(TripleUtil.S_OWL_ON_PROPERTY) }
        .map { case (v, (_, p)) => (v, p) }.cache()

      val hasValueRDD = tripleRDD
        .filter { case (_, (p, _)) => p.equals(TripleUtil.S_OWL_HAS_VALUE) }
        .map { case (v, (_, w)) => (v, w) }.cache()

      val joinedHasValOnPropRDD = onPropRDD.join(hasValueRDD)

      val joinedHasValOnPropMap1 = sparkContext.broadcast {
        joinedHasValOnPropRDD.map { case (v, pw) => (pw, v) }
          .groupByKey()
          .collectAsMap()
      }

      val joinedHasValOnPropMap2: Broadcast[Map[String, scala.Iterable[(String, String)]]] = sparkContext.broadcast {
        joinedHasValOnPropRDD
          .groupByKey()
          .collectAsMap()
      }

      val someValueRDD = tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_OWL_SOME_VALUES_FROM) }
        .map { case (v, (_, w)) => (v, w) }.cache()

      val joinedSomValOnPropMap: Broadcast[Map[(String, String), Iterable[String]]] = sparkContext.broadcast {
        someValueRDD.join(onPropRDD)
          .map { case (v, (w, p)) => ((w, p), v) }
          .groupByKey()
          .collectAsMap()
      }

      val allValueRDD = tripleRDD.filter { case (_, (p, _)) => p.equals(TripleUtil.S_OWL_ALL_VALUES_FROM) }
        .map { case (v, (_, w)) => (v, w) }.cache()

      val joinedAllValOnPropMap = sparkContext.broadcast {
        allValueRDD.join(onPropRDD)
          .map { case (v, (w, p)) => ((v, p), w) }
          .groupByKey()
          .collectAsMap()
      }

      joinedHasValOnPropRDD.unpersist(true)
      someValueRDD.unpersist(true)
      allValueRDD.unpersist(true)
      onPropRDD.unpersist(true)
      hasValueRDD.unpersist(true)

      /* Instance Reasoning */

      val filteredTranProps0 = tripleRDD.mapPartitions(filterTranProp(bcTransPropArray), true)
      filteredTranProps0.count()
      var hasValuePropCount = 1L

      val typeRDD = sparkContext
        .textFile(inputFile)
        .map(parseTriple)
        .filter { case (_, (p, _)) => p.equals(TripleUtil.S_RDF_TYPE) }
        .map { case (s, (_, o)) => (s, o) }
        .partitionBy(new HashPartitioner(slices))
        .persist(StorageLevel.MEMORY_ONLY)

      println("Number of input types : " + typeRDD.count())

      var instanceIteration = 0
      var iteration = 0
      val emptyArray1: Array[(String, (String, String))] = Array()

      var tempTripleRDD: RDD[(String, (String, String))] = sparkContext.parallelize(emptyArray1)
      var newTripleRDD: RDD[(String, (String, String))] = sparkContext.parallelize(emptyArray1)

      val emptyArray2: Array[(String, String)] = Array()

      var newTypeRDD: RDD[(String, String)] = sparkContext.parallelize(emptyArray2)

      var hasValCount = -1L
      var newTripleCount = -1L
      var tempTripleCount = -1L

      while (hasValCount != 0L) {
        
        var tempTripleRDD2: RDD[(String, (String, String))] = sparkContext.parallelize(emptyArray1)

        while (newTripleCount != 0L) {

          if (iteration == 0 && instanceIteration == 0) {

            val newTripleRDD1 = tripleRDD.mapPartitions(subpropInhReasoning(bcSubpropMap)).partitionBy(partitioner)
            newTripleRDD1.count()
            val newTripleRDD2 = transitivePropClosure(filteredTranProps0, sparkContext).subtract(filteredTranProps0).partitionBy(partitioner)
            newTripleRDD2.count()
            val newTripleRDD3 = tripleRDD.mapPartitions(inverseReasoning1(bcInverseOfMap1)).partitionBy(partitioner)
            newTripleRDD3.count()
            val newTripleRDD4 = tripleRDD.mapPartitions(inverseReasoning2(bcInverseOfMap2)).partitionBy(partitioner)
            newTripleRDD4.count()
            val newTripleRDD5 = tripleRDD.mapPartitions(symmetricReasoning(bcSymmetricPropMap)).partitionBy(partitioner)
            newTripleRDD4.count()
            val unionedNewTripleRDD = sparkContext
              .union(newTripleRDD1, newTripleRDD3, newTripleRDD4, newTripleRDD5, newTripleRDD2)
            unionedNewTripleRDD.count()

            tempTripleRDD = unionedNewTripleRDD.subtract(tripleRDD).distinct().partitionBy(partitioner)
            tempTripleCount = tempTripleRDD.count()
            tempTripleRDD2 = tempTripleRDD
            tempTripleRDD2.count()

          } else {

            val newTripleRDD1 = tempTripleRDD.mapPartitions(subpropInhReasoning(bcSubpropMap)).partitionBy(partitioner)
            newTripleRDD1.count()

            val filteredTranProps1 = newTripleRDD.mapPartitions(filterTranProp(bcTransPropArray), true)
            filteredTranProps1.count()
            val filteredTranProps2 = tempTripleRDD2.mapPartitions(filterTranProp(bcTransPropArray), true)
            filteredTranProps2.count()
            val filteredTranProps = filteredTranProps0.union(filteredTranProps1).union(filteredTranProps2)
            filteredTranProps.count()
            val newTripleRDD2 = transitivePropClosure(filteredTranProps, sparkContext)
              .subtract(filteredTranProps).partitionBy(partitioner)

            newTripleRDD2.count()
            val newTripleRDD3 = tempTripleRDD.mapPartitions(inverseReasoning1(bcInverseOfMap1)).partitionBy(partitioner)
            newTripleRDD3.count()
            val newTripleRDD4 = tempTripleRDD.mapPartitions(inverseReasoning2(bcInverseOfMap2)).partitionBy(partitioner)
            newTripleRDD4.count()
            val newTripleRDD5 = tempTripleRDD.mapPartitions(symmetricReasoning(bcSymmetricPropMap)).partitionBy(partitioner)
            newTripleRDD5.count()
            val unionedNewTripleRDD = sparkContext
              .union(newTripleRDD1, newTripleRDD3, newTripleRDD4, newTripleRDD5, newTripleRDD2)
            unionedNewTripleRDD.count()

            tempTripleRDD = unionedNewTripleRDD.subtract(tripleRDD).subtract(tempTripleRDD2).subtract(newTripleRDD).partitionBy(partitioner)
            newTripleCount = tempTripleRDD.count()

            tempTripleRDD2 = tempTripleRDD2.union(tempTripleRDD)
            tempTripleRDD2.count()
          }

          instanceIteration += 1
          println("instance iteration: " + instanceIteration)
        }

        instanceIteration = 0
        var typeIteration = 0

        tempTripleRDD = sparkContext.parallelize(emptyArray1)

        var oldCount = newTypeRDD.count()
        var newCount = -1L

        while (oldCount != newCount) {
          oldCount = newTypeRDD.count()

          if (iteration == 0 && typeIteration == 0) {

            val domRDD1 = tripleRDD.mapPartitions(domainReasoning(bcDomainMap), true).partitionBy(partitioner)
            domRDD1.count()
            val domRDD2 = tempTripleRDD2.mapPartitions(domainReasoning(bcDomainMap), true).partitionBy(partitioner)
            domRDD2.count()

            val ranRDD1 = tripleRDD.mapPartitions(rangeReasoning(bcRangeMap), true).partitionBy(partitioner)
            ranRDD1.count()
            val ranRDD2 = tempTripleRDD2.mapPartitions(rangeReasoning(bcRangeMap), true).partitionBy(partitioner)
            ranRDD2.count()

            val hasValRDD1 = tripleRDD.mapPartitions(hasValueReasoning1(joinedHasValOnPropMap1), true).partitionBy(partitioner)
            hasValRDD1.count()
            val hasValRDD2 = tempTripleRDD2.mapPartitions(hasValueReasoning1(joinedHasValOnPropMap1), true).partitionBy(partitioner)
            hasValRDD2.count()

            val tempTypeRDD1 = sparkContext.union(domRDD1, domRDD2, ranRDD1, ranRDD2, hasValRDD1, hasValRDD2)
              .subtract(typeRDD)
              .distinct()
              .partitionBy(partitioner)
            tempTypeRDD1.count()

            // SomeValues Reasoning

            val filteredSomeValOnPropTriples = {
              val filteredTriples1 = tripleRDD.mapPartitions(filterSomValOnPropTriple(joinedSomValOnPropMap), true).partitionBy(partitioner)
              filteredTriples1.count()
              val filteredTriples2 = tempTripleRDD2.mapPartitions(filterSomValOnPropTriple(joinedSomValOnPropMap), true).partitionBy(partitioner)
              filteredTriples2.count()
              sparkContext.union(filteredTriples1, filteredTriples2)
            }
            filteredSomeValOnPropTriples.count()

            val filteredSomeValOnPropTypes = {
              val filteredTypes1 = typeRDD.mapPartitions(filterSomValOnPropType(joinedSomValOnPropMap), true).partitionBy(partitioner)
              filteredTypes1.count()
              val filteredTypes2 = tempTypeRDD1.mapPartitions(filterSomValOnPropType(joinedSomValOnPropMap), true).partitionBy(partitioner)
              filteredTypes2.count()
              filteredTypes1.union(filteredTypes2)
            }
            filteredSomeValOnPropTypes.count()

            val newSomeValueTypes = filteredSomeValOnPropTriples.join(filteredSomeValOnPropTypes)
              .mapPartitions(someValueReasoning(joinedSomValOnPropMap), true)
              .subtract(typeRDD)
              .subtract(tempTypeRDD1)
              .distinct()
              .partitionBy(partitioner)
            newSomeValueTypes.count()

            // AllValues Reasoning

            val filteredAllValOnPropTriples = {
              val filteredTriples1 = tripleRDD.mapPartitions(filterAllValOnPropTriple(joinedAllValOnPropMap), true).partitionBy(partitioner)
              filteredTriples1.count()
              val filteredTriples2 = tempTripleRDD2.mapPartitions(filterAllValOnPropTriple(joinedAllValOnPropMap), true).partitionBy(partitioner)
              filteredTriples2.count()
              sparkContext.union(filteredTriples1, filteredTriples2)
            }
            filteredAllValOnPropTriples.count()

            val filteredAllValOnPropTypes = {
              val filteredTypes1 = typeRDD.mapPartitions(filterAllValOnPropType(joinedAllValOnPropMap), true).partitionBy(partitioner)
              filteredTypes1.count()
              val filteredTypes2 = tempTypeRDD1.mapPartitions(filterAllValOnPropType(joinedAllValOnPropMap), true).partitionBy(partitioner)
              filteredTypes2.count()
              filteredTypes1.union(filteredTypes2)
            }
            filteredAllValOnPropTypes.count()

            val newAllValueTypes = filteredAllValOnPropTriples.join(filteredAllValOnPropTypes)
              .mapPartitions(allValueReasoning(joinedSomValOnPropMap), true)
              .subtract(typeRDD)
              .subtract(tempTypeRDD1)
              .subtract(newSomeValueTypes)
              .partitionBy(partitioner)

            newAllValueTypes.count()

            val newSubclassTypes = {
              val newSubclassTypes1 = tempTypeRDD1.mapPartitions(subclassReasoning(bcSubclassMap), true).partitionBy(partitioner)
              newSubclassTypes1.count()
              val newSubclassTypes2 = newSomeValueTypes.mapPartitions(subclassReasoning(bcSubclassMap), true).partitionBy(partitioner)
              newSubclassTypes2.count()
              val newSubclassTypes3 = newAllValueTypes.mapPartitions(subclassReasoning(bcSubclassMap), true).partitionBy(partitioner)
              newSubclassTypes3.count()
              sparkContext.union(newSubclassTypes1, newSubclassTypes2, newSubclassTypes3)
                .subtract(typeRDD)
                .subtract(tempTypeRDD1)
                .subtract(newSomeValueTypes)
                .subtract(newAllValueTypes)
                .partitionBy(partitioner)
            }
            newSubclassTypes.count()

            newTypeRDD = sparkContext.union(tempTypeRDD1, newSomeValueTypes, newAllValueTypes, newSubclassTypes)
              .distinct()
              .partitionBy(partitioner)

            newCount = newTypeRDD.count()


            val newHasValTripleRDD = {
              val newHasValType1 = typeRDD.mapPartitions(hasValueReasoning2(joinedHasValOnPropMap2), true).partitionBy(partitioner)
              newHasValType1.count()
              val newHasValType2 = newTypeRDD.mapPartitions(hasValueReasoning2(joinedHasValOnPropMap2), true).partitionBy(partitioner)
              newHasValType2.count()

              sparkContext.union(newHasValType1, newHasValType2).subtract(tripleRDD).subtract(tempTripleRDD2).partitionBy(partitioner)
            }

            newTripleRDD = tempTripleRDD2.union(newHasValTripleRDD)

            hasValCount = newHasValTripleRDD.count()

            newTripleRDD.count()
            tempTripleRDD = newHasValTripleRDD
            tempTripleRDD.count()
            tempTripleRDD2 = newHasValTripleRDD
            tempTripleRDD2.count()

          } else {

            val domRDD = tempTripleRDD2.mapPartitions(domainReasoning(bcDomainMap), true).partitionBy(partitioner)
            domRDD.count()

            val ranRDD = tempTripleRDD2.mapPartitions(rangeReasoning(bcRangeMap), true).partitionBy(partitioner)
            ranRDD.count()

            val hasValRDD = tempTripleRDD2.mapPartitions(hasValueReasoning1(joinedHasValOnPropMap1), true).partitionBy(partitioner)
            hasValRDD.count()

            val tempTypeRDD1 = sparkContext.union(domRDD, ranRDD, hasValRDD)
              .subtract(typeRDD)
              .subtract(newTypeRDD)
              .partitionBy(partitioner)
            tempTypeRDD1.count()

            // SomeValues Reasoning

            val filteredSomeValOnPropTriples = {
              val filteredTriples1 = tripleRDD.mapPartitions(filterSomValOnPropTriple(joinedSomValOnPropMap), true).partitionBy(partitioner)
              filteredTriples1.count()
              val filteredTriples2 = newTripleRDD.mapPartitions(filterSomValOnPropTriple(joinedSomValOnPropMap), true).partitionBy(partitioner)
              filteredTriples2.count()
              sparkContext.union(filteredTriples1, filteredTriples2)
            }
            filteredSomeValOnPropTriples.count()

            val filteredSomeValOnPropTypes = {
              val filteredTypes1 = typeRDD.mapPartitions(filterSomValOnPropType(joinedSomValOnPropMap), true).partitionBy(partitioner)
              filteredTypes1.count()
              val filteredTypes2 = tempTypeRDD1.mapPartitions(filterSomValOnPropType(joinedSomValOnPropMap), true).partitionBy(partitioner)
              filteredTypes2.count()
              val filteredTypes3 = newTypeRDD.mapPartitions(filterSomValOnPropType(joinedSomValOnPropMap), true).partitionBy(partitioner)
              filteredTypes3.count()
              sparkContext.union(filteredTypes1, filteredTypes2, filteredTypes3)
            }
            filteredSomeValOnPropTypes.count()

            val newSomeValueTypes = filteredSomeValOnPropTriples.join(filteredSomeValOnPropTypes)
              .mapPartitions(someValueReasoning(joinedSomValOnPropMap), true)
              .distinct()
              .subtract(typeRDD)
              .subtract(newTypeRDD)
              .subtract(tempTypeRDD1)
              .partitionBy(partitioner)
            newSomeValueTypes.count()

            // AllValues Reasoning

            val filteredAllValOnPropTriples = {
              val filteredTriples1 = tripleRDD.mapPartitions(filterAllValOnPropTriple(joinedAllValOnPropMap), true).partitionBy(partitioner)
              filteredTriples1.count()
              val filteredTriples2 = newTripleRDD.mapPartitions(filterAllValOnPropTriple(joinedAllValOnPropMap), true).partitionBy(partitioner)
              filteredTriples2.count()
              sparkContext.union(filteredTriples1, filteredTriples2)
            }
            filteredAllValOnPropTriples.count()

            val filteredAllValOnPropTypes = {
              val filteredTypes1 = typeRDD.mapPartitions(filterAllValOnPropType(joinedAllValOnPropMap), true).partitionBy(partitioner)
              filteredTypes1.count()
              val filteredTypes2 = tempTypeRDD1.mapPartitions(filterAllValOnPropType(joinedAllValOnPropMap), true).partitionBy(partitioner)
              filteredTypes2.count()
              val filteredTypes3 = newTypeRDD.mapPartitions(filterAllValOnPropType(joinedAllValOnPropMap), true).partitionBy(partitioner)
              filteredTypes3.count()
              filteredTypes1.union(filteredTypes2).union(filteredTypes3)
            }
            filteredAllValOnPropTypes.count()

            val newAllValueTypes = filteredAllValOnPropTriples.join(filteredAllValOnPropTypes)
              .mapPartitions(allValueReasoning(joinedAllValOnPropMap), true)
              .distinct()
              .subtract(typeRDD)
              .subtract(newSomeValueTypes)
              .subtract(tempTypeRDD1)
              .subtract(newTypeRDD)
              .partitionBy(partitioner)
            newAllValueTypes.count()

            val newSubclassTypes = {
              val newSubclassTypes1 = tempTypeRDD1.mapPartitions(subclassReasoning(bcSubclassMap), true).partitionBy(partitioner)
              newSubclassTypes1.count()
              val newSubclassTypes2 = newSomeValueTypes.mapPartitions(subclassReasoning(bcSubclassMap), true).partitionBy(partitioner)
              newSubclassTypes2.count()
              val newSubclassTypes3 = newAllValueTypes.mapPartitions(subclassReasoning(bcSubclassMap), true).partitionBy(partitioner)
              newSubclassTypes3.count()
              sparkContext.union(newSubclassTypes1, newSubclassTypes2, newSubclassTypes3)
                .subtract(typeRDD)
                .subtract(newTypeRDD)
                .subtract(tempTypeRDD1)
                .subtract(newSomeValueTypes)
                .subtract(newAllValueTypes)
            }
            newSubclassTypes.count()

            val tempTypes = sparkContext.union(tempTypeRDD1, newSomeValueTypes, newAllValueTypes, newSubclassTypes)
              .distinct()
              .partitionBy(partitioner)
            tempTypes.count()

            newTypeRDD = sparkContext.union(newTypeRDD, tempTypes)
            newCount = newTypeRDD.count()

            val newHasValTripleRDD =
              tempTypes.mapPartitions(hasValueReasoning2(joinedHasValOnPropMap2), true)
                .subtract(tripleRDD).subtract(newTripleRDD).subtract(tempTripleRDD)
                .distinct.partitionBy(partitioner)

            newHasValTripleRDD.count()
            newTripleRDD = newTripleRDD.union(newHasValTripleRDD)
            newTripleRDD.count()

            tempTripleRDD = tempTripleRDD.union(newHasValTripleRDD)
            hasValCount = tempTripleRDD.count()
            tempTripleRDD2 = newHasValTripleRDD

          }
          typeIteration += 1
          println("type iteration : " + typeIteration)
        }
        iteration += 1

      }

      val bcInvFuncPropArray: Broadcast[Array[String]] = sparkContext.broadcast {
        tripleRDD.filter { case (_, (p, o)) =>
          p.equals(TripleUtil.S_RDF_TYPE) && o.equals(TripleUtil.S_OWL_INVERSE_FUNCTIONAL_PROPERTY)
        }
          .map { case (p, _) => p }
          .distinct()
          .collect()
      }

      val bcFuncPropArray: Broadcast[Array[String]] = sparkContext.broadcast {
        tripleRDD.filter { case (_, (p, o)) =>
          p.equals(TripleUtil.S_RDF_TYPE) && o.equals(TripleUtil.S_OWL_FUNCTIONAL_PROPERTY)
        }
          .map { case (p, _) => p }
          .distinct()
          .collect()
      }

      val sameasRDD = sparkContext
        .textFile(inputFile)
        .map(parseTriple)
        .filter { case (_, (p, _)) => p.equals(TripleUtil.S_OWL_SAME_AS) }
        .map { case (s, (_, o)) => (s, o) }
        .distinct()
        .partitionBy(partitioner)
        .persist(StorageLevel.MEMORY_ONLY)
      sameasRDD.count()

      val sameasInvFuncRDD = {
        val triple1 = tripleRDD.mapPartitions(filterFuncProp(bcFuncPropArray), true)
        triple1.count()
        val triple2 = newTripleRDD.mapPartitions(filterFuncProp(bcFuncPropArray), true)
        triple2.count()
        sparkContext.union(triple1, triple2)
          .combineByKey(createCombiner, mergeValue, mergeCombiners) //function prop reasoning
          .flatMap { case (_, list) => combinator(list) }
          .distinct()
          .partitionBy(partitioner)
      }
      sameasInvFuncRDD.count()

      val sameasFuncPropRDD = {
        val triple1 = tripleRDD.mapPartitions(filterFuncProp(bcInvFuncPropArray), true)
        triple1.count()
        val triple2 = newTripleRDD.mapPartitions(filterFuncProp(bcInvFuncPropArray), true)
        triple2.count()
        sparkContext.union(triple1, triple2)
          .combineByKey(createCombiner, mergeValue, mergeCombiners) // = groupByKey (the same functionity)
          .flatMap { case (_, list) => combinator(list) }
          .distinct()
          .partitionBy(partitioner)
      }
      sameasFuncPropRDD.count()

      var newSameasRDD = sameasInvFuncRDD.union(sameasFuncPropRDD)
        .distinct().subtract(sameasRDD).partitionBy(partitioner)
      newSameasRDD.count()

      newSameasRDD = sameasRDD.union(newSameasRDD)
      newSameasRDD.count()
      newSameasRDD = newSameasRDD.union(newSameasRDD.map { case (v, w) => (w, v) }).distinct()
      newSameasRDD.count()

      newSameasRDD = transitiveClosure(newSameasRDD, sparkContext).subtract(sameasRDD)


      println("new instance triples : " + newTripleRDD.count())
      println("new type triples: " + newTypeRDD.count())
      println("new sameas triples : " + newSameasRDD.count())

      newTripleRDD.map { case (s, (p, o)) => s + " " + p + " " + o + " ."}.saveAsTextFile(args(1) + "/new_instances")
      newTypeRDD.map { case (s, o) => s + " " + TripleUtil.S_RDF_TYPE + " " + o + " ."}.saveAsTextFile(args(1) + "/new_types")
      newSameasRDD.map { case (s, o) => s + " " + TripleUtil.S_OWL_SAME_AS + " " + o + " ."}.saveAsTextFile(args(1) + "/new_sameas")

      println("runtime : " + ((System.currentTimeMillis() - startTime) / 60000.0).toString + " min")
    } catch {
      case e: Exception => println(e)
    } finally {
      sparkContext.stop()
    }
  }
}
