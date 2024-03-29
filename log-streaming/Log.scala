package com.logstreaming

/** An entry of Apache access log. */
case class Log(
  ipAddress: String,
  clientIdentd: String,
  userId: String,
  dateTime: String,
  method: String,
  endpoint: String,
  protocol: String,
  responseCode: Int,
  contentSize: Long) extends Serializable {
}

object Log {
  private val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  /**
    * Parse log entry from a string.
    *
    * @param log A string, typically a line from a log file
    * @return An entry of Apache access log, an empty value if cannot parse the line
    */
  def parseLogLine(log: String): Option[Log] = {
    log match {
      case PATTERN(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode, contentSize)
      => Some(Log(ipAddress,
        clientIdentd,
        userId,
        dateTime,
        method,
        endpoint,
        protocol,
        responseCode.toInt,
        contentSize.toLong))
      case _ => None
    }
  }
}
