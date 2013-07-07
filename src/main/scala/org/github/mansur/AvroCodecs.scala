package org.github.mansur

import com.twitter.bijection.Injection
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecordBase}
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import com.twitter.bijection.Inversion.attempt
import com.twitter.bijection.Attempt
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.Schema
import org.apache.avro.io.{DecoderFactory, DatumReader, EncoderFactory, DatumWriter}

/**
 * @author Muhammad Ashraf
 * @since 7/4/13
 */
object AvroCodecs {
  def apply[T <: SpecificRecordBase : Manifest]: Injection[T, Array[Byte]] = {
    val klass = manifest[T].erasure.asInstanceOf[Class[T]]
    new SpecificAvroCodec[T](klass)
  }

  def apply[T <: GenericRecord](schema: Schema): Injection[T, Array[Byte]] = {
    new GenericAvroCodec[T](schema)
  }

  def toBinary[T <: SpecificRecordBase : Manifest]: Injection[T, Array[Byte]] = {
    val klass = manifest[T].erasure.asInstanceOf[Class[T]]
    val writer = new SpecificDatumWriter[T](klass)
    val reader = new SpecificDatumReader[T](klass)
    new BinaryAvroCodec[T](writer, reader)
  }

  def toBinary[T <: GenericRecord](schema: Schema): Injection[T, Array[Byte]] = {
    val writer = new GenericDatumWriter[T](schema)
    val reader = new GenericDatumReader[T](schema)
    new BinaryAvroCodec[T](writer, reader)
  }

  def toJson[T <: GenericRecord](schema: Schema): Injection[T, Array[Byte]] = {
    val writer = new GenericDatumWriter[T](schema)
    val reader = new GenericDatumReader[T](schema)
    new JsonAvroCodec[T](schema, writer, reader)
  }

  def toJson[T <: SpecificRecordBase : Manifest](schema: Schema): Injection[T, Array[Byte]] = {
    val klass = manifest[T].erasure.asInstanceOf[Class[T]]
    val writer = new SpecificDatumWriter[T](klass)
    val reader = new SpecificDatumReader[T](klass)
    new JsonAvroCodec[T](schema, writer, reader)
  }
}

class SpecificAvroCodec[T <: SpecificRecordBase](klass: Class[T]) extends Injection[T, Array[Byte]] {
  def apply(a: T): Array[Byte] = {
    val writer = new SpecificDatumWriter[T](a.getSchema)
    val fileWriter = new DataFileWriter[T](writer)
    val stream = new ByteArrayOutputStream()
    fileWriter.create(a.getSchema, stream)
    fileWriter.append(a)
    fileWriter.flush()
    stream.toByteArray
  }

  def invert(bytes: Array[Byte]): Attempt[T] = attempt(bytes) {
    bytes =>
      val reader = new SpecificDatumReader[T](klass)
      val stream = new DataFileStream[T](new ByteArrayInputStream(bytes), reader)
      val result = stream.next()
      stream.close()
      result
  }
}

class GenericAvroCodec[T <: GenericRecord](schema: Schema) extends Injection[T, Array[Byte]] {
  def apply(a: T): Array[Byte] = {
    val writer = new GenericDatumWriter[T](a.getSchema)
    val fileWriter = new DataFileWriter[T](writer)
    val stream = new ByteArrayOutputStream()
    fileWriter.create(a.getSchema, stream)
    fileWriter.append(a)
    fileWriter.flush()
    stream.toByteArray
  }

  def invert(bytes: Array[Byte]): Attempt[T] = attempt(bytes) {
    bytes =>
      val reader = new GenericDatumReader[T](schema)
      val stream = new DataFileStream[T](new ByteArrayInputStream(bytes), reader)
      val result = stream.next()
      stream.close()
      result
  }
}

class BinaryAvroCodec[T](writer: DatumWriter[T], reader: DatumReader[T]) extends Injection[T, Array[Byte]] {
  def apply(a: T): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val binaryEncoder = EncoderFactory.get().binaryEncoder(stream, null)
    writer.write(a, binaryEncoder)
    binaryEncoder.flush()
    stream.toByteArray
  }

  def invert(bytes: Array[Byte]): Attempt[T] = attempt(bytes) {
    bytes =>
      val binaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
      reader.read(null.asInstanceOf[T], binaryDecoder)
  }
}

class JsonAvroCodec[T](schema: Schema, writer: DatumWriter[T], reader: DatumReader[T]) extends Injection[T, Array[Byte]] {
  def apply(a: T): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().jsonEncoder(schema, stream)
    writer.write(a, encoder)
    encoder.flush()
    stream.toByteArray
  }

  def invert(bytes: Array[Byte]): Attempt[T] = attempt(bytes) {
    bytes =>
      val decoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(bytes))
      reader.read(null.asInstanceOf[T], decoder)
  }
}
