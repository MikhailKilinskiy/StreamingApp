package org.bigdata.source

import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable
import com.github.javafaker.Faker
import java.sql.Timestamp

import org.bigdata.models.StockDataGen

sealed abstract class Symbols(val value: Int) extends IntEnumEntry

object Symbols extends IntEnum[Symbols] {
  override def values: immutable.IndexedSeq[Symbols] = findValues

  case object APPL extends Symbols(1)
  case object GOOG extends Symbols(2)
  case object MSFT extends Symbols(3)
}

sealed abstract class Currency(val value: Int) extends IntEnumEntry

object Currency extends IntEnum[Currency] {
  override def values: immutable.IndexedSeq[Currency] = findValues

  case object USD extends Currency(1)
  case object EUR extends Currency(2)
}

sealed abstract class Exchange(val value: Int) extends IntEnumEntry

object Exchange extends IntEnum[Exchange] {
  override def values: immutable.IndexedSeq[Exchange] = findValues

  case object NASDAQ extends Exchange(1)
  case object NYSE extends Exchange(2)
}


object DataGenerator {
  private val faker = new Faker

  def GenerateData(): StockDataGen = {
    val symbol = Symbols.withValue(faker.number.numberBetween(1, 4))
    val name = symbol match {
      case Symbols.APPL => "Apple"
      case Symbols.GOOG => "Google"
      case Symbols.MSFT => "Microsoft"
    }
    val currency = Currency.withValue(faker.number.numberBetween(1, 2))
    val exchange = Exchange.withValue(faker.number.numberBetween(1, 2))

    new StockDataGen(
      symbol.toString,
      name,
      currency.toString,
      exchange.toString,
      faker.numerify("###.##").toFloat,
      faker.number.numberBetween(5, 100000),
      new Timestamp(System.currentTimeMillis())
    )
  }
}

