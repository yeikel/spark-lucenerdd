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
package org.zouzias.spark.lucenerdd.aggregate

import com.twitter.algebird.{MapMonoid, Monoid}
import org.zouzias.spark.lucenerdd.models.SparkFacetResult

class SparkFacetResultMonoid(facetName: String) extends Monoid[SparkFacetResult] {

  override def zero: SparkFacetResult = SparkFacetResult(facetName, Map.empty[String, Long])

  override def plus(l: SparkFacetResult, r: SparkFacetResult): SparkFacetResult =
    SparkFacetResultMonoid.plus(l, r)
}


object SparkFacetResultMonoid extends Serializable {
  private lazy val facetMonoid = new MapMonoid[String, Long]()

  def zero(facetName: String): SparkFacetResult = SparkFacetResult(facetName, facetMonoid.zero)
  def plus(l: SparkFacetResult, r: SparkFacetResult): SparkFacetResult = {
    require(l.facetName == r.facetName)
    SparkFacetResult(l.facetName, facetMonoid.plus(l.facets, r.facets))
  }
}