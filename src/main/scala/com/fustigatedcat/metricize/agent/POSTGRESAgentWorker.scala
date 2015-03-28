package com.fustigatedcat.metricize.agent

import com.fustigatedcat.metricize.agent.intf.{AgentWorkerInterface, AgentResponse}
import com.fustigatedcat.metricize.agent.postgres.POSTGRESWorker
import com.typesafe.config.Config

class POSTGRESAgentWorker(config : Config) extends AgentWorkerInterface {

  Class.forName("org.postgresql.Driver")

  val fqdn = config.getString("fqdn")

  val username = config.getString("username")

  val password = config.getString("password")

  val port = config.getString("port")

  val queryString = config.getString("queryString")

  val dbName = config.getString("dbName")

  val jdbc = s"jdbc:postgresql://$fqdn:$port/$dbName"

  def needsRescheduling_? : Boolean = true

  def process(): AgentResponse = {
    POSTGRESWorker.executeMysqlWorker(jdbc, username, password, queryString)
  }

}
