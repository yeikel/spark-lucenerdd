/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zouzias.spark.lucenerdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class LuceneRDDDataFrameMultivalues extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  var luceneRDD: LuceneRDD[_] = _

  "Search on Array" should "should work" in {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val entities = Seq(
      ("123",Seq("Peter", "Johan"),"123"),
      ("125",Seq("Peter"),"123"),
      ("1234",Seq("Marc", "Johan"),"210")
    ).toDF("id","names","address").coalesce(1)

    val entitiesRDD = LuceneRDD(entities)


    def fuzzyLinker(row: Row): Query = {

      val names: Seq[String] = row.getAs("names")
      val address: String = row.getAs("address")
      val booleanQuery = new BooleanQuery.Builder

      names.foreach(s => {
        val query = new TermQuery(new Term("names", s.toLowerCase()))
        booleanQuery.add(query, BooleanClause.Occur.MUST)
      })

      val addressQ = new TermQuery(new Term("address", address.toLowerCase()))
      booleanQuery.add(addressQ, BooleanClause.Occur.MUST)

      val query = booleanQuery.build()
      println(query)

      query
    }

    val linkedResults = entitiesRDD.link(entitiesRDD, fuzzyLinker, 100)

    val linkageResults = spark.createDataFrame(linkedResults
      .map { case (left, topDocs) =>

        (
         topDocs.map(s => s.doc.textField("names")),
         topDocs.map(s => s.doc.textFieldSingle("address")).headOption,
        left.getAs[Seq[String]]("names"),
        left.getAs[String]("address"))
      })
      .toDF("results","age", "base","age_base")

    linkageResults.show(false)


  }
}
