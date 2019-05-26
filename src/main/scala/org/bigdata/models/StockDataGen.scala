package org.bigdata.models

import java.sql.Timestamp
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonFormat

sealed trait Data extends Product with Serializable { }


case class StockDataGen (
                          symbol: String,
                          name: String,
                          currency: String,
                          stockExchange: String,
                          price: Float,
                          volume: Long,
                          @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
                          last_trade_time: Timestamp
                        ) extends Data
