package io.dronekit

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 9/16/15.
 *
 */
import java.util.NoSuchElementException

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.document.{JsonDocument, RawJsonDocument}
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.view.{AsyncViewRow, Stale, ViewQuery}
import rx.RxReactiveStreams
import rx.lang.scala.JavaConversions.{toJavaObservable, toScalaObservable}
import rx.lang.scala.Observable
import spray.json.{DefaultJsonProtocol, _}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 7/31/15.
 *
 * Wrapper class to convert the RxJava interface to Akka-Streams
 *
 */

case class DocumentResponse[T](cas: Long, entity: T)

object CouchbaseStreamsWrapper {
  // Keep the env in an object so it is only created once
  val env = DefaultCouchbaseEnvironment.builder().build()
}

class CouchbaseStreamsWrapper(host: String, bucketName: String, password: String, protocol: DefaultJsonProtocol)
                    (implicit ec: ExecutionContext, mat: ActorMaterializer)
{
  // Reuse the env here
  val cluster = CouchbaseCluster.create(CouchbaseStreamsWrapper.env, host)
  val bucket = cluster.openBucket(bucketName, password)

  /**
   * Convert an Observable with a single element into a Future
   * @param observable The source to convert
   * @tparam T The type of the resulting Future
   * @return The future
   */
  def observableToFuture[T](observable: Observable[T]): Future[T] = {
    Source(RxReactiveStreams.toPublisher(observable)).runWith(Sink.head)
  }

  /**
   * Convert an RxJava observable into an Akka-Streams Source
   * @param observable The observable to convert from
   * @tparam T The type parameter of the observable/source
   * @return An Akka-Streams Source
   */
  def observableToSource[T](observable: rx.lang.scala.Observable[T]): Source[T, Any] = {
    Source(RxReactiveStreams.toPublisher(observable))
  }

  /**
   * Convert an entity of type T into a json string
   * @param entity The entity to convert from
   * @param format The json format implicit to use for conversion
   * @tparam T The type of the entity to convert from
   * @return A string encoding of the entity in json format
   */
  private def marshalEntity[T](entity: T)(implicit format: JsonFormat[T]): String = {
    entity.toJson.compactPrint
  }

  /**
   * Using spray-json, convert the found document into an entity of type T
   * @param docObservable The observable to get the json document from
   * @param format The json format implicit to use for conversion
   * @tparam T The type parameter to convert to
   * @return A DocumentResponse with the entity and the current CAS value
   */
  private def convertToEntity[T](docObservable: Observable[RawJsonDocument])
                                (implicit format: JsonFormat[T]): Future[DocumentResponse[T]] = {
    val docPublisher = RxReactiveStreams.toPublisher(docObservable)
    val rawDocFuture = Source(docPublisher).runWith(Sink.head)
    rawDocFuture.map { rawJsonDoc =>
      val entity = rawJsonDoc.content().parseJson.convertTo[T]
      DocumentResponse(cas = rawJsonDoc.cas(), entity = entity)
    }
  }

  private def convertToString(docObservable: Observable[RawJsonDocument]): Future[DocumentResponse[String]] = {
    val docPublisher = RxReactiveStreams.toPublisher(docObservable)
    val rawDocFuture = Source(docPublisher).runWith(Sink.head)
    rawDocFuture.map { rawString =>
      val entity = rawString.content()
      DocumentResponse(cas = rawString.cas(), entity = entity)
    }
  }

  /**
   * Using spray-json, convert the found document into an entity of type T
   * @param jsonDocument The document retrieved
   * @param format The json format implicit to use for conversion
   * @tparam T The type parameter to convert to
   * @return A DocumentResponse with the entity and the current CAS value
   */
  private def convertToEntity[T](jsonDocument: RawJsonDocument)(implicit format: JsonFormat[T]): DocumentResponse[T] = {
    val entity = jsonDocument.content().parseJson.convertTo[T]
    DocumentResponse(cas = jsonDocument.cas(), entity = entity)
  }

  /**
   * Replace a document in couchbase with the given entity by marshalling it to
   * json, then returning the unmarshalled json returned from couchbase
   * @param entity Overwrite the document in the database with this entity
   * @param key The document to overwrite
   * @tparam T The type of the entity
   * @return The replaced document in the database
   */
  def replaceDocument[T](entity: T, key: String, cas: Long)(implicit format: JsonFormat[T]): Future[DocumentResponse[T]] = {
    val jsonString = marshalEntity[T](entity)
    val doc = RawJsonDocument.create(key, jsonString, cas)
    val replaceObservable = bucket.async().replace(doc)
    convertToEntity[T](replaceObservable)
  }


  /**
   * Marshall entity to json, insert it into the database, then unmarshal the response and return it
   * @param entity The object to insert
   * @param key The location to insert it to
   * @param expiry optional expiry time in seconds, 0 (stored indefinitely) if not set
   * @tparam T The type of the object
   * @return The resulting document after it has been inserted
   */
  def insertDocument[T](entity: T, key: String, expiry: Int = 0)
                       (implicit format: JsonFormat[T]): Future[DocumentResponse[T]] = {
    val jsonString = marshalEntity[T](entity)
    val doc = RawJsonDocument.create(key, expiry, jsonString)
    val insertObservable = bucket.async().insert(doc)
    convertToEntity[T](insertObservable)
  }

  def insertRawString(data: String, key: String, expiry: Int = 0)
                      : Future[DocumentResponse[String]] = {
    val doc = RawJsonDocument.create(key, expiry, data)
    val insertObservable = bucket.async().insert(doc)
    convertToString(insertObservable)
  }

  /**
   * Lookup a document in couchbase by the key, and unmarshal it into an object: T
   * @param key The key to lookup in the database
   * @tparam T The type to unmarshal the returned document to
   * @return A Future with the object T if found, otherwise None
   */
  def lookupByKey[T](key: String)(implicit format: JsonFormat[T]): Future[DocumentResponse[T]] = {
    val docObservable = bucket.async().get(key, classOf[RawJsonDocument])
    convertToEntity[T](docObservable)
  }

  def lookupStringByKey(key: String): Future[DocumentResponse[String]] = {
    val docObservable = bucket.async().get(key, classOf[RawJsonDocument])
    convertToString(docObservable)
  }

  /**
   * Retrieve a list of keys in Couchbase using the batch async system
   * @param keys The list of keys to retrieve
   * @return A Source of RawJsonDocuments
   */
  def batchLookupByKey[T](keys: List[String])
                         (implicit format: JsonFormat[T]):
  Source[DocumentResponse[T], Any] = {
    val docObservable = Observable.from(keys)
      .flatMap(key => bucket.async().get(key, classOf[RawJsonDocument]))
    observableToSource(docObservable).map(json => convertToEntity[T](json))
  }


  /**
   * Remove a document from the database
   * @param key The document to remove
   * @return A Successful future if the document was found, otherwise a Failure
   */
  def removeByKey(key: String): Future[JsonDocument] = {
    val removedObservable = bucket.async().remove(key)
    val publisher = RxReactiveStreams.toPublisher(removedObservable)
    Source(publisher).runWith(Sink.head)
  }

  /**
   * Extract an entity of type T from an AsyncViewRow
   * @param row The AyncViewRow to extract the entity from
   * @tparam T The type of the entity to unmarshal from the row
   * @return The unmarshalled entity if successful
   */
  def getEntityFromRow[T](row: AsyncViewRow)
                         (implicit format: JsonFormat[T]):
  Future[DocumentResponse[T]] = {
    convertToEntity[T](row.document(classOf[RawJsonDocument]))
  }

  /**
   * Query the designDoc/viewDoc for the given key, and return the resulting row if found
   * @param designDoc The design document containing the view
   * @param viewDoc The view to query
   * @param key The key to lookup in the query
   * @param forceIndex Set to true to force the view index to update on request. This should be used carefully
   * @return The row containing the key if found
   */
  def indexQuerySingleElement(designDoc: String, viewDoc: String, key: String, forceIndex: Boolean=false):
  Future[Option[AsyncViewRow]] = {
    val staleState = if (forceIndex) Stale.FALSE else Stale.TRUE
    val rowFuture = observableToFuture(
      indexQuery(designDoc, viewDoc, List(key), staleState).head)

    // Check if a row was returned, and return None if it was not found else return the row
    val resultPromise = Promise[Option[AsyncViewRow]]()
    rowFuture.onComplete {
      case Success(row) => resultPromise.success(Some(row))
      case Failure(ex: NoSuchElementException) => resultPromise.success(None)
      case Failure(ex) => resultPromise.failure(ex)
    }
    resultPromise.future
  }

  /**
   * Query an index for multiple items
   * @param designDoc The design document name to query
   * @param viewDoc The view in that design doc
   * @param keys The list of keys to query for
   * @param stale Allow potentially stale indexes or not
   * @return Observable of AsyncViewRow
   */
  def indexQuery(designDoc: String, viewDoc: String, keys: List[String] = List(), stale: Stale = Stale.FALSE):
  Observable[AsyncViewRow] = {
    // Couchbase needs a java.util.List
    val keyList: java.util.List[String] = keys
    val query =
      if (keys.isEmpty) ViewQuery.from(designDoc, viewDoc).stale(stale).inclusiveEnd(true)
      else ViewQuery.from(designDoc, viewDoc).stale(stale).inclusiveEnd(true).keys(JsonArray.from(keyList))
    toScalaObservable(bucket.async().query(query))
      .flatMap(queryResult => queryResult.rows())
  }

  /**
   * Query an index with a compound key
   * @param designDoc The name of the design document
   * @param viewDoc The name of the view
   * @param keys A List of lists to query for
   * @param stale Allow stale records
   * @return Observable of AsyncViewRows
   */
  def compoundIndexQuery(designDoc: String, viewDoc: String, keys: List[List[Any]], stale: Stale = Stale.FALSE):
  Observable[AsyncViewRow] = {
    // Couchbase needs a java.util.List
    val keyList: java.util.List[java.util.List[Any]] = keys.map(seqAsJavaList)
    val query = ViewQuery.from(designDoc, viewDoc).stale(stale).inclusiveEnd(true).keys(JsonArray.from(keyList))
    toScalaObservable(bucket.async().query(query))
      .flatMap(queryResult => queryResult.rows())
  }

  /**
   * Get the documents found from an Observable[AsyncViewRow]
   * @param docObservable An Observable[AsyncViewRow] as the list of documents
   * @return A new Observable[RawJsonDocument]
   */
  def withDocuments(docObservable: rx.lang.scala.Observable[AsyncViewRow]): Observable[RawJsonDocument] = {
    docObservable.flatMap(_.document(classOf[RawJsonDocument]))
  }


  /**
   * Query an index and unmarshal the found documents into an entity of type T
   * @param designDoc The name of the design document to query
   * @param viewDoc The name of the view
   * @param keys The keys to query for
   * @param stale Allow stale records or not
   * @tparam T The entity type to unmarshal to
   * @return A Source[T, Any] of the found documents.
   */
  def indexQueryToEntity[T](designDoc: String, viewDoc: String, keys: List[String] = List(), stale: Stale = Stale.FALSE)
                           (implicit format: JsonFormat[T]): Source[DocumentResponse[T], Any] = {
    val query = indexQuery(designDoc, viewDoc, keys, stale)
    val docs = withDocuments(query)
    Source(RxReactiveStreams.toPublisher(docs)).map(convertToEntity[T])
  }

  /**
   * Query a compound index and unmarshal the found documents into an entity of type T
   * @param designDoc The name of the design document
   * @param viewDoc The name of the view
   * @param keys The list of keys to query for, where each compound key is a list of keys
   * @param stale Allow stale records or not
   * @tparam T The type of the entity to unmarshal to
   * @return A Source[T, Any] of the found documents.
   */
  def compoundIndexQueryToEntity[T](designDoc: String, viewDoc: String, keys: List[List[Any]], stale: Stale = Stale.FALSE)
                                   (implicit format: JsonFormat[T]):
  Source[DocumentResponse[T], Any] = {
    val query = compoundIndexQuery(designDoc, viewDoc, keys, stale)
    val docs = withDocuments(query)
    Source(RxReactiveStreams.toPublisher(docs)).map(convertToEntity[T])
  }

}
