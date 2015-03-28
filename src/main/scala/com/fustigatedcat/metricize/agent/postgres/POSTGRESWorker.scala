package com.fustigatedcat.metricize.agent.postgres

import java.sql.{DriverManager, ResultSetMetaData, ResultSet}

import com.fustigatedcat.metricize.agent.intf.{AgentResponseFailure, AgentResponseSuccess}
import org.json4s.JsonAST.{JValue, JArray, JObject}
import org.json4s.JsonDSL._
import org.json4s.native.{prettyJson, renderJValue}

object POSTGRESWorker {

  def handleResultSet(rs : ResultSet, rsmd : ResultSetMetaData, out : List[JObject] = List()) : JArray = rs.next() match {
    case true => {
      val arr = JObject((for(col <- 0 until rsmd.getColumnCount) yield {
        rsmd.getColumnName(col + 1) -> (rs.getString(col + 1) : JValue)
      }).toList)
      handleResultSet(
        rs,
        rsmd,
        arr :: out
      )
    }
    case _ => out.reverse
  }

  def executeMysqlWorker(jdbc : String, username : String, password : String, query : String) = {
    var start = System.currentTimeMillis()
    try {
      val conn = DriverManager.getConnection(jdbc, username, password)
      val stmt = conn.createStatement
      start = System.currentTimeMillis()
      val rs = stmt.executeQuery(query)
      val end = System.currentTimeMillis()
      val rtn = handleResultSet(rs, rs.getMetaData)
      conn.close()
      AgentResponseSuccess(start, end - start, prettyJson(renderJValue(rtn)))
    } catch {
      case err: Throwable => AgentResponseFailure(start, System.currentTimeMillis() - start, err.getLocalizedMessage)
    }
  }

}
