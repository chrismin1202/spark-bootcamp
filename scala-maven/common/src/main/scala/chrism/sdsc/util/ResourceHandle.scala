/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package chrism.sdsc.util

import scala.io.Source

object ResourceHandle {

  def loadResource(path: String, encoding: String = "UTF-8"): Iterator[String] =
    Source
      .fromInputStream(getClass.getResourceAsStream(path), encoding)
      .getLines()

  /** From Beginning Scala by David Pollak.
    *
    * @param param of type A; passed to func
    * @param func  any function or whatever that matches the signature
    * @tparam A any type with {{{def close(): Unit}}}: Unit; Java's Closeable interface should be compatible
    * @tparam B any type including Any
    * @return of type B
    */
  def using[A <: { def close(): Unit }, B](param: A)(func: A => B): B =
    try func(param)
    finally
    // close even when an Exception is caught
    if (param != null)
      param.close()
}
