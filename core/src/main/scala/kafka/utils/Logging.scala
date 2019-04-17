/**
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

package kafka.utils

import com.typesafe.scalalogging.Logger
import edu.brown.cs.systems.xtrace.XTrace
import edu.brown.cs.systems.xtrace.logging.XTraceLogger
import org.slf4j.{LoggerFactory, Marker, MarkerFactory}


object Log4jControllerRegistration {
  private val logger = Logger(this.getClass.getName)

  try {
    val log4jController = Class.forName("kafka.utils.Log4jController").asInstanceOf[Class[Object]]
    val instance = log4jController.getDeclaredConstructor().newInstance()
    CoreUtils.registerMBean(instance, "kafka:type=kafka.Log4jController")
    logger.info("Registered kafka:type=kafka.Log4jController MBean")
  } catch {
    case _: Exception => logger.info("Couldn't register kafka:type=kafka.Log4jController MBean")
  }
}

private object Logging {
  private val FatalMarker: Marker = MarkerFactory.getMarker("FATAL")
}

trait Logging {

  private val xtrace: XTraceLogger = XTrace.getLogger(Logging.getClass)

  protected lazy val logger = Logger(LoggerFactory.getLogger(loggerName))

  protected var logIdent: String = _

  Log4jControllerRegistration

  protected def loggerName: String = getClass.getName

  protected def msgWithLogIdent(msg: String): String =
    if (logIdent == null) msg else logIdent + msg

  def trace(msg: => String): Unit = {
    logger.trace(msgWithLogIdent(msg))
    xtrace.log("trace---\t" + msg)
  }

  def trace(msg: => String, e: => Throwable): Unit = {
    logger.trace(msgWithLogIdent(msg),e)
    xtrace.log("trace---\t" + msg)
  }

  def isDebugEnabled: Boolean = logger.underlying.isDebugEnabled

  def isTraceEnabled: Boolean = logger.underlying.isTraceEnabled

  def debug(msg: => String): Unit = {
    logger.debug(msgWithLogIdent(msg))
    xtrace.log("debug---\t" + msg)
  }

  def debug(msg: => String, e: => Throwable): Unit = {
    logger.debug(msgWithLogIdent(msg),e)
    xtrace.log(msg)
  }

  def info(msg: => String): Unit = {
    logger.info(msgWithLogIdent(msg))
    xtrace.log("info---\t" + msg)
  }

  def info(msg: => String,e: => Throwable): Unit = {
    logger.info(msgWithLogIdent(msg),e)
    xtrace.log("info---\t" + msg)
  }

  def warn(msg: => String): Unit = {
    logger.warn(msgWithLogIdent(msg))
    xtrace.log("warn---\t" + msg)
  }

  def warn(msg: => String, e: => Throwable): Unit = {
    logger.warn(msgWithLogIdent(msg),e)
    xtrace.log("warn---\t" + msg)
  }

  def error(msg: => String): Unit = {
    logger.error(msgWithLogIdent(msg))
    xtrace.log("error---\t" + msg)
  }

  def error(msg: => String, e: => Throwable): Unit = {
    logger.error(msgWithLogIdent(msg),e)
    xtrace.log("error---\t" + msg)
  }

  def fatal(msg: => String): Unit = {
    logger.error(Logging.FatalMarker, msgWithLogIdent(msg))
    xtrace.log("fatal---\t" + msg)
  }


  def fatal(msg: => String, e: => Throwable): Unit = {
    logger.error(Logging.FatalMarker, msgWithLogIdent(msg), e)
    xtrace.log("fatal---\t" + msg)
  }

}
