package org.github.mansur

import org.specs._
import org.github.mansur.avro.FiscalRecord
import com.twitter.bijection.Injection

/**
 * @author Muhammad Ashraf
 * @since 7/6/13
 */
object SpecificAvroCodecTest extends Specification with BaseProperties {
  "Aa" should {
    "Avro test" in {
      Injection
      val testRecord = FiscalRecord.newBuilder()
        .setCalendarDate("1")
        .setFiscalWeek(1)
        .setFiscalYear(2)
        .build()
      val bytes = AvroCodecs[FiscalRecord].apply(testRecord)
      val result = AvroCodecs[FiscalRecord].invert(bytes)
      result must_== testRecord
    }
  }
}
