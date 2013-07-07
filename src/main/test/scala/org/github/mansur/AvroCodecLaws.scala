package org.github.mansur

import com.twitter.bijection.Injection
import org.github.mansur.avro.FiscalRecord
import org.scalacheck.Properties
import org.apache.avro.generic.{GenericRecord, GenericData}
import org.apache.avro.Schema

/**
 * @author Muhammad Ashraf
 * @since 7/5/13
 */
object AvroCodecLaws extends Properties("AvroCodecs") with BaseProperties {
  val testSchema = new Schema.Parser().parse( """{
                                                   "type":"record",
                                                   "name":"FiscalRecord",
                                                   "namespace":"org.github.mansur.avro",
                                                   "fields":[
                                                      {
                                                         "name":"calendarDate",
                                                         "type":"string"
                                                      },
                                                      {
                                                         "name":"fiscalWeek",
                                                         "type":[
                                                            "int",
                                                            "null"
                                                         ]
                                                      },
                                                      {
                                                         "name":"fiscalYear",
                                                         "type":[
                                                            "int",
                                                            "null"
                                                         ]
                                                      }
                                                   ]
                                                }""")


  def buildSpecificAvroRecord(i: (String, Int, Int)): FiscalRecord = {
    FiscalRecord.newBuilder()
      .setCalendarDate(i._1)
      .setFiscalWeek(i._2)
      .setFiscalYear(i._3)
      .build()
  }

  def buildGenericAvroRecord(i: (String, Int, Int)): GenericRecord = {

    val fiscalRecord = new GenericData.Record(testSchema)
    fiscalRecord.put("calendarDate", i._1)
    fiscalRecord.put("fiscalWeek", i._2)
    fiscalRecord.put("fiscalYear", i._3)
    fiscalRecord
  }

  implicit val testSpecificRecord = arbitraryViaFn {
    is: (String, Int, Int) => buildSpecificAvroRecord(is)
  }

  implicit val testGenericRecord = arbitraryViaFn {
    is: (String, Int, Int) => buildGenericAvroRecord(is)
  }

  def roundTripsSpecificRecord(implicit injection: Injection[FiscalRecord, Array[Byte]]) = {
    isLooseInjection[FiscalRecord, Array[Byte]]
  }

  def roundTripsGenericRecord(implicit injection: Injection[GenericRecord, Array[Byte]]) = {
    isLooseInjection[GenericRecord, Array[Byte]]
  }

  property("round trips Specific Record -> Array[Byte]") =
    roundTripsSpecificRecord(AvroCodecs[FiscalRecord])

  property("round trips Generic Record -> Array[Byte]") =
    roundTripsGenericRecord(AvroCodecs[GenericRecord](testSchema))

  property("round trips Specific Record -> Array[Byte] using Binary Encoder/Decoder") =
    roundTripsSpecificRecord(AvroCodecs.toBinary[FiscalRecord])

  property("round trips Generic Record -> Array[Byte] using Binary Encoder/Decoder") =
    roundTripsGenericRecord(AvroCodecs.toBinary[GenericRecord](testSchema))

  property("round trips Generic Record -> Array[Byte] using Json Encoder/Decoder") =
    roundTripsGenericRecord(AvroCodecs.toJson[GenericRecord](testSchema))

  property("round trips Specific Record -> Array[Byte] using Json Encoder/Decoder") =
    roundTripsSpecificRecord(AvroCodecs.toJson[FiscalRecord](testSchema))

}


