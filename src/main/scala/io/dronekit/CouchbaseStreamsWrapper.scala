package io.dronekit

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 9/16/15.
 *
 */

import java.util.NoSuchElementException

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.couchbase.client.deps.io.netty.buffer._
import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil
import com.couchbase.client.java.{ReplicateTo, PersistTo, CouchbaseCluster}
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.document.{BinaryDocument, JsonDocument, RawJsonDocument}
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.query.Select._
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.{Expression, Sort}
import com.couchbase.client.java.view.{AsyncViewRow, Stale, ViewQuery}
import com.typesafe.scalalogging.Logger
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.slf4j.LoggerFactory
import rx.RxReactiveStreams
import rx.lang.scala.JavaConversions.{toJavaObservable, toScalaObservable}
import rx.lang.scala.Observable
import spray.json.{DefaultJsonProtocol, _}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 7/31/15.
 *
 * Wrapper class to convert the RxJava interface to Akka-Streams
 *
 */


/**
  * Wrapper to allow returning the CAS value of an object along with the object itself
  *
  * @param cas The current Check And Set value of the queried object
  * @param entity The contents of the document
  * @tparam T The type of the contents
  */
case class DocumentResponse[T](cas: Long, entity: T)

/**
  * Wraps returned values in a DocumentResponse
  *
  * @param rows A Source of DocumentResponse objects which includes the CAS value
  * @param status The status of the query as reported by Couchbase
  * @param errors Any errors from the query as reported by Couchbase
  * @param info Metrics on the query as reported by Couchbase
  * @tparam T The type of the entity
  */
case class QueryResponse[T](rows: Source[DocumentResponse[T], Any],
                         status: Future[String],
                         errors: Future[JsonObject],
                         info: Future[N1qlMetrics])

/**
  * Wraps returned values in a DocumentResponse
  *
  * @param rows A Source of DocumentResponse objects which includes the CAS value
  * @param errors Any errors from the query as reported by Couchbase
  * @param debug Metrics on the query as reported by Couchbase
  * @tparam T The type of the entity
  */
case class ViewQueryResponse[T](rows: Source[DocumentResponse[T], Any],
                            errors: Future[JsonObject],
                            debug: JsonObject)

/**
  * Returned values are AsyncN1qlQueryRow(s), no CAS value is returned unless the query included it
  *
  * @param rows The results of the query
  * @param status The status of the query as reported by Couchbase
  * @param errors Any errors from the query as reported by Couchbase
  * @param info Metrics on the query as reported by Couchbase
  */
case class RawQueryResponse(rows: Source[AsyncN1qlQueryRow, Any],
                         status: Future[String],
                         errors: Future[JsonObject],
                         info: Future[N1qlMetrics])

case class UpdateObject[T](entity: T, cas: Long, key: String)

class DocumentNotFound(message: String) extends RuntimeException(message)

object CouchbaseStreamsWrapper {
  // Keep the env in an object so it is only created once
  val env = DefaultCouchbaseEnvironment.builder().build()
  /**
    * A static key to query views by date. This key can be used as the start key for any date.
    * The compound key is composed of year, month, day, hour, minute, second
    */
  val DateStartKey = Seq[Int](0, 0, 0, 0, 0, 0)
  /**
    * A static key to query views by date. This key can be used as the end key for any date.
    * The compound key is composed of year, month, day, hour, minute, second
    */
  val DateEndKey = Seq[Int](9999, 99, 99, 99, 99, 99)
}


/**
  * A wrapper around a specific bucket which provides an Akka-Streams translation to the RxJava couchbase java API.
  *
  * @param host Hostname of the couchbase server to connect to
  * @param bucketName Name of the bucket to connect to
  * @param password The password for the bucket
  * @param protocol A Spray-Json protocol to marshall/unmarshall documents to case classes
  * @param ec Execution Context for Futures and Streams
  * @param mat Materializer for Akka-Streams
  */
class CouchbaseStreamsWrapper(host: String, bucketName: String, password: String, protocol: DefaultJsonProtocol)
                    (implicit ec: ExecutionContext, mat: ActorMaterializer)
{
  // Reuse the env here
  val cluster = CouchbaseCluster.create(CouchbaseStreamsWrapper.env, host)
  val bucket = cluster.openBucket(bucketName, password)
  val log: Logger = Logger(LoggerFactory.getLogger(getClass))

  /**
   * Convert an Observable with a single element into a Future
    *
    * @param observable The source to convert
   * @tparam T The type of the resulting Future
   * @return The future
   */
  private def observableToFuture[T](observable: Observable[T], empty: Option[T] = None): Future[T] = {
    val p = Promise[T]
    toScalaObservable(observable).subscribe(
      value => p.trySuccess(value),
      e => p.tryFailure(e),
      () =>
        if (empty.isDefined) p.trySuccess(empty.get)
        else p.tryFailure(new RuntimeException("No content"))
    )
    p.future
  }

  /**
   * Convert an RxJava observable into an Akka-Streams Source
    *
    * @param observable The observable to convert from
   * @tparam T The type parameter of the observable/source
   * @return An Akka-Streams Source
   */
  private def observableToSource[T](observable: rx.lang.scala.Observable[T]): Source[T, Any] = {
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(observable)))
  }

  /**
   * Convert an entity of type T into a json string
    *
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
    *
    * @param docObservable The observable to get the json document from
   * @param format The json format implicit to use for conversion
   * @tparam T The type parameter to convert to
   * @return A DocumentResponse with the entity and the current CAS value
   */
  private def convertToEntity[T](docObservable: Observable[RawJsonDocument])
                                (implicit format: JsonFormat[T]): Future[DocumentResponse[T]] = {
    val p = Promise[DocumentResponse[T]]
    toScalaObservable(docObservable).subscribe(
      doc => p.trySuccess(DocumentResponse(cas = doc.cas(), entity = doc.content().parseJson.convertTo[T])),
      e => p.tryFailure(e),
      () => p.tryFailure(new DocumentNotFound(""))
    )
    p.future
  }

  /**
   * Using spray-json, convert the found document into an entity of type T
    *
    * @param jsonDocument The document retrieved
   * @param format The json format implicit to use for conversion
   * @tparam T The type parameter to convert to
   * @return A DocumentResponse with the entity and the current CAS value
   */
  private def convertToEntity[T](jsonDocument: RawJsonDocument)(implicit format: JsonFormat[T]): DocumentResponse[T] = {
    try {
      val entity = jsonDocument.content().parseJson.convertTo[T]
      DocumentResponse(cas = jsonDocument.cas(), entity = entity)
    } catch {
      case ex: DeserializationException =>
        log.error(s"Caught $ex when converting:\n ${jsonDocument.content()}")
        throw ex
    }
  }

  /**
   * Replace a document in couchbase with the given entity by marshalling it to
   * json, then returning the unmarshalled json returned from couchbase
    *
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
    *
    * @param entity The object to insert
   * @param key The location to insert it to
   * @param expiry optional expiry time in seconds, 0 (stored indefinitely) if not set
   * @tparam T The type of the object
   * @return The resulting document after it has been inserted
   */
  def insertDocument[T](entity: T, key: String, expiry: Int = 0,
                        persist: PersistTo = PersistTo.NONE, replicate: ReplicateTo = ReplicateTo.NONE)
                       (implicit format: JsonFormat[T]): Future[DocumentResponse[T]] = {
    val jsonString = marshalEntity[T](entity)
    val doc = RawJsonDocument.create(key, expiry, jsonString)
    val insertObservable = bucket.async().insert(doc)
    convertToEntity[T](insertObservable)
  }

  /**
   * Lookup a document in couchbase by the key, and unmarshal it into an object: T
    *
    * @param key The key to lookup in the database
   * @tparam T The type to unmarshal the returned document to
   * @return A Future with the object T if found, otherwise None
   */
  def lookupByKey[T](key: String)(implicit format: JsonFormat[T]): Future[DocumentResponse[T]] = {
    val docObservable = bucket.async().get(key, classOf[RawJsonDocument])
    convertToEntity[T](docObservable)
  }

  def binaryLookupByKey(key: String): Future[DocumentResponse[ByteString]] = {
    val p = Promise[DocumentResponse[ByteString]]()
    val docObservable = bucket.async().get(key, classOf[BinaryDocument])
    toScalaObservable(docObservable).subscribe(
      doc => {
        p.trySuccess(DocumentResponse(cas = doc.cas(), entity = ByteString(doc.content().nioBuffer())))
        ReferenceCountUtil.release(doc.content())
      },
      e => p.tryFailure(e),
      () => p.tryFailure(new DocumentNotFound(""))
    )
    p.future
  }

  def binaryInsertDocument(data: ByteString, key: String, expiry: Int = 0): Future[DocumentResponse[ByteString]] = {
    val p = Promise[DocumentResponse[ByteString]]()
    val buffer = Unpooled.copiedBuffer(data.toArray[Byte])
    val doc = BinaryDocument.create(key, expiry, buffer)
    val insertObservable = toScalaObservable(bucket.async().insert(doc))
    insertObservable.subscribe(
      doc => p.success(DocumentResponse(cas = doc.cas(), entity = ByteString())),
      e => p.failure(e)
    )
    p.future
  }

  /**
   * Retrieve a list of keys in Couchbase using the batch async system
    *
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
   * Does a batch update of keys -> each update
   * Throws a DocumentNotFound exception if all of the keys for the batch updates cannot be found
   * @param entities Seqence of UpdateObject to update
   * @return a Future source of entities from the batch update request
   */
  def batchUpdate[T](entities: Seq[UpdateObject[T]])(implicit format: JsonFormat[T]): Future[Source[DocumentResponse[T], Any]] = {
    val lookupSource = batchLookupByKey[T](entities.map{e => e.key}.toList)
    lookupSource.grouped(entities.length).runWith(Sink.head).map{res =>
      val Expected = entities.length
      res.length match {
        case Expected =>
          val observableSeq = entities.map{e =>
            val replacement = RawJsonDocument.create(e.key, marshalEntity[T](e.entity), e.cas)
            bucket.async().replace(replacement)
          }

          val observableCombined = observableSeq.reduce((ob1, ob2) => ob1.mergeWith(ob2))
          observableToSource(observableCombined).map(json => convertToEntity[T](json))
        case _ => throw new DocumentNotFound(s"Could not perform a batch update due to missing keys")
      }
    }
  }


  /**
   * Remove a document from the database
    *
    * @param key The document to remove
   * @return A Successful future if the document was found, otherwise a Failure
   */
  def removeByKey(key: String): Future[JsonDocument] = {
    val p = Promise[JsonDocument]()
    val removedObservable = bucket.async().remove(key)
    toScalaObservable(removedObservable).subscribe(
      doc => p.trySuccess(doc),
      e => p.tryFailure(e),
      () => p.tryFailure(new DocumentNotFound(""))
    )
    p.future
  }

  /**
   * Extract an entity of type T from an AsyncViewRow
    *
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
    * Create an array suitable for doing date queries on couchbase
    *
    * @param date The DateTime object to convert into a date array
    * @return A sequence to use as a compound key in a couchbase view query
    */
  def getDateKey(date: DateTime): Seq[Int] = {
    if (date.getZone != UTC) throw new IllegalArgumentException("DateTime must be in UTC timezone")
    Seq(
      date.year().get(),
      date.monthOfYear().get(),
      date.dayOfMonth().get(),
      date.hourOfDay().get(),
      date.minuteOfHour().get(),
      date.secondOfMinute().get())
  }

  /**
   * Query the designDoc/viewDoc for the given key, and return the resulting row if found
    *
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
    *
    * @param designDoc The design document name to query
   * @param viewDoc The view in that design doc
   * @param keys The list of keys to query for
   * @param stale Allow potentially stale indexes or not
   * @return Observable of AsyncViewRow
   */
  def indexQuery(designDoc: String, viewDoc: String, keys: List[String] = List(), stale: Stale = Stale.FALSE,
                limit: Int = Int.MaxValue, skip: Int = 0): Observable[AsyncViewRow] = {
    // Couchbase needs a java.util.List
    val keyList: java.util.List[String] = keys.asJava
    val query =
      if (keys.isEmpty)
        ViewQuery.from(designDoc, viewDoc)
          .stale(stale)
          .inclusiveEnd(true)
          .limit(limit)
          .skip(skip)
      else
        ViewQuery.from(designDoc, viewDoc)
          .stale(stale)
          .inclusiveEnd(true)
          .keys(JsonArray.from(keyList))
          .limit(limit)
          .skip(skip)
    toScalaObservable(bucket.async().query(query))
      .flatMap(queryResult => queryResult.rows())
  }

  /**
   * Query an index with a compound key
    *
    * @param designDoc The name of the design document
   * @param viewDoc The name of the view
   * @param keys A List of lists to query for
   * @param stale Allow stale records
   * @return Observable of AsyncViewRows
   */
  def compoundIndexQuery(designDoc: String, viewDoc: String, keys: Option[List[List[Any]]] = None,
                         startKey: Option[Seq[Any]] = None, endKey: Option[Seq[Any]] = None,
                         stale: Stale = Stale.FALSE, limit: Int = Int.MaxValue, skip: Int = 0): Observable[AsyncViewRow] = {
    // Couchbase needs a java.util.List
    var query = ViewQuery
      .from(designDoc, viewDoc)
      .stale(stale)
      .inclusiveEnd(true)
      .limit(limit)
      .skip(skip)

    if (keys.nonEmpty) {
      val keyList: java.util.List[java.util.List[Any]] = keys.get.map(_.asJava).asJava
      query = query.keys(JsonArray.from(keyList))
    }

    if (startKey.nonEmpty) {
      query = query.startKey(JsonArray.from(startKey.get.asJava))
    }

    if (endKey.nonEmpty) {
      query = query.endKey(JsonArray.from(endKey.get.asJava))
    }

    toScalaObservable(bucket.async().query(query))
      .flatMap(queryResult => queryResult.rows())
  }

  def paginatedIndexQuery[T](designDoc: String, viewDoc: String, startKey: Seq[Any], endKey: Seq[Any],
                          startDocId: Option[String] = None, stale: Stale = Stale.FALSE, limit: Int = 100)
                         (implicit format: JsonFormat[T]): Future[ViewQueryResponse[T]] = {
    if (startKey.length != endKey.length) throw new IllegalArgumentException("startKey and endKey must be the same length")

    val query = ViewQuery
      .from(designDoc, viewDoc)
      .stale(stale)
      .startKey(JsonArray.from(startKey.asJava))
      .endKey(JsonArray.from(endKey.asJava))
      .startKeyDocId(startDocId.getOrElse(""))
      .limit(limit)
      .skip(if (startDocId.isDefined) 1 else 0)
      .includeDocs(classOf[RawJsonDocument])
    observableToFuture(bucket.async().query(query))
      .map { queryResult =>
        val rows = toScalaObservable(queryResult.rows())
          .flatMap(row => row.document(classOf[RawJsonDocument]))
          .map(rawDoc => convertToEntity[T](rawDoc))
        ViewQueryResponse[T](
          rows = observableToSource(rows),
          errors = observableToFuture(queryResult.error(), Some(JsonObject.empty())),
          debug = queryResult.debug()
        )
      }
  }

  /**
   * Get the documents found from an Observable[AsyncViewRow]
    *
    * @param docObservable An Observable[AsyncViewRow] as the list of documents
   * @return A new Observable[RawJsonDocument]
   */
  def withDocuments(docObservable: rx.lang.scala.Observable[AsyncViewRow]): Observable[RawJsonDocument] = {
    docObservable.flatMap(_.document(classOf[RawJsonDocument]))
  }


  /**
   * Query an index and unmarshal the found documents into an entity of type T
    *
    * @param designDoc The name of the design document to query
   * @param viewDoc The name of the view
   * @param keys The keys to query for
   * @param stale Allow stale records or not
   * @tparam T The entity type to unmarshal to
   * @return A Source[T, Any] of the found documents.
   */
  def indexQueryToEntity[T](designDoc: String, viewDoc: String, keys: List[String] = List(), stale: Stale = Stale.FALSE,
                           limit: Int = Int.MaxValue, skip: Int = 0)
                           (implicit format: JsonFormat[T]): Source[DocumentResponse[T], Any] = {
    val query = indexQuery(designDoc, viewDoc, keys, stale, limit, skip)
    val docs = withDocuments(query)
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(docs))).map(convertToEntity[T])
  }

  /**
   * Query a compound index by a set of keys and unmarshal the found documents into an entity of type T
   *
   * @param designDoc The name of the design document
   * @param viewDoc The name of the view
   * @param keys The list of keys to query for, where each compound key is a list of keys
   * @param stale Allow stale records or not
   * @tparam T The type of the entity to unmarshal to
   * @return A Source[T, Any] of the found documents.
   */
  def compoundIndexQueryByKeysToEntity[T](designDoc: String, viewDoc: String, keys: Option[List[List[Any]]] = None,
                                    stale: Stale = Stale.FALSE, limit: Int = Int.MaxValue, skip: Int = 0)
                                   (implicit format: JsonFormat[T]):
  Source[DocumentResponse[T], Any] = {
    val query = compoundIndexQuery(designDoc, viewDoc, keys, None, None, stale, limit, skip)
    val docs = withDocuments(query)
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(docs))).map(convertToEntity[T])
  }

  /**
   * Query a compound index with a start & end range and unmarshal the results
   * @param designDoc The name of the design document
   * @param viewDoc The name of the view
   * @param startKey compound start key range, optional
   * @param endKey compound end key range, optional
   * @param stale Allow stale records or not
   * @param limit number of results to check, defaults to Int.Max
   * @param skip number of results to skip, defaults to 0
   * @tparam T type of entity to unmarshall to
   * @return A Source[T, Any] of the found documents.
   */
  def compoundIndexQueryByRangeToEntity[T](designDoc: String, viewDoc: String, startKey: Option[Seq[Any]] = None,
                                           endKey: Option[Seq[Any]] = None, stale: Stale = Stale.FALSE,
                                           limit: Int = Int.MaxValue, skip: Int = 0)
                                          (implicit format: JsonFormat[T]):
  Source[DocumentResponse[T], Any] = {
    val query = compoundIndexQuery(designDoc, viewDoc, None, startKey, endKey, stale, limit, skip)
    val docs = withDocuments(query)
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(docs))).map(convertToEntity[T])
  }

  /**
    * Query couchbase using the N1QL interface. There must be an index created on the bucket for this to succeed.
    *
    * Comment: This is a leaky abstraction around the Couchbase N1QL interface, but it didn't seem worth it to
    *          attempt to re-create their query DSL. An alternative is to just accept strings for queries, but
    *          that doesn't seem like a better solution either.
    *
    * @param q The query statement to execute. You *MUST* select meta().cas
    * @param params Optional query parameters for N1QL. Most important is the .adhoc() option, which should always be set to
    *               false for production queries which are executed frequently. This is the default behavior.
    *               Also important is the ScanConsistency option (similar to the Stale option on view queries).
    * @return A QueryResponse with the rows, as well as info, status, and errors from the query.
    */
  def n1qlQuery(q: Statement, params: N1qlParams = N1qlParams.build().adhoc(false)): Future[RawQueryResponse] = {
    val query = N1qlQuery.simple(q, params)
    val resultObservable = bucket.async().query(query)
    val observable = toScalaObservable(resultObservable)
      .map{ queryResult =>
        RawQueryResponse(
          observableToSource[AsyncN1qlQueryRow](queryResult.rows()),
          status = observableToFuture[String](queryResult.status()),
          errors = observableToFuture[JsonObject](queryResult.errors(), Some(JsonObject.empty())),
          info = observableToFuture[N1qlMetrics](queryResult.info())
        )
      }
    observableToFuture[RawQueryResponse](observable)
  }

  /**
    * Query couchbase using the N1QL interface, and convert the found rows into Case Classes of type T.
    * There must be an index created on this bucket to succeed. This interface always uses select(*), since
    * otherwise it is not possible to convert the resulting rows into T (since they will be missing fields).
    *
    * Comment: This is a leaky abstraction around the Couchbase N1QL interface, but it didn't seem worth it to
    *          attempt to re-create their query DSL. An alternative is to just accept strings for queries, but
    *          that doesn't seem like a better solution either.
    *
    * @param where The expressing selecting which records to return.
    * @param order The order to return the records, and the field to sort by
    * @param params Optional query parameters for N1QL. Most important is the .adhoc() option, which should always be
    *               set to false for production queries which are executed frequently. This is the default behavior.
    * @param limit The maximum number of rows to return
    * @param offset The number of rows to skip
    * @param format The JSON protocol to convert the found rows into case classes.
    * @tparam T The case class type to convert to.
    * @return A QueryResponse with the rows, as well as info, status, and errors from the query.
    */
  def n1qlQueryToEntity[T](where: Expression, order: Sort, params: N1qlParams = N1qlParams.build().adhoc(false),
                           limit: Int = 100, offset: Int = 0)
                          (implicit format: JsonFormat[T]): Future[QueryResponse[T]] = {
    val s: Statement = select("*, meta().cas")
      // Need to wrap bucket name with i() because of the - in the name
      .from(i(bucketName))
      .where(where)
      .orderBy(order)
      .limit(limit)
      .offset(offset)
    val q = N1qlQuery.simple(s, params)
    val observable = toScalaObservable(bucket.async().query(q))
      .map{ queryResult => QueryResponse[T](
        rows = observableToSource[AsyncN1qlQueryRow](queryResult.rows())
          .map{ row =>
            val cas = row.value().getLong("cas")
            val entity = row.value().getObject(bucketName).toString.parseJson.convertTo[T]
            DocumentResponse[T](cas, entity)},
        status = observableToFuture[String](queryResult.status()),
        errors = observableToFuture(queryResult.errors()),
        info = observableToFuture[N1qlMetrics](queryResult.info())
      )}
    observableToFuture[QueryResponse[T]](observable)
  }

  /**
    * Query couchbase with a parameterized query
    *
    * @param query The parameterized query to run
    * @return A RawQueryResponse
    */
  def n1qlParameterizedQuery(query: ParameterizedN1qlQuery): Future[RawQueryResponse] = {
    observableToFuture(bucket.async().query(query))
      .map { queryResult =>
        RawQueryResponse(
          observableToSource[AsyncN1qlQueryRow](queryResult.rows()),
          status = observableToFuture[String](queryResult.status()),
          errors = observableToFuture[JsonObject](queryResult.errors(), Some(JsonObject.empty())),
          info = observableToFuture[N1qlMetrics](queryResult.info())
        )
      }
  }

  /**
    * Query couchbase with a parameterized query, and extract entities from the results. You must use
    * select * for this to work, or the entity will be missing fields.
    *
    * @param query The parameterized query to run
    * @tparam T The entity type
    * @return A QueryResponse with the results.
    */
  def n1qlParameterizedQueryToEntity[T](query: ParameterizedN1qlQuery)
                                       (implicit format: JsonFormat[T]): Future[QueryResponse[T]] = {
    observableToFuture(bucket.async().query(query))
      .map{ queryResult => QueryResponse[T](
        rows = observableToSource[AsyncN1qlQueryRow](queryResult.rows())
          .map{ row =>
            val cas = row.value().getLong("cas")
            val entity = row.value().getObject(bucketName).toString.parseJson.convertTo[T]
            DocumentResponse[T](cas, entity)},
        status = observableToFuture[String](queryResult.status()),
        errors = observableToFuture(queryResult.errors()),
        info = observableToFuture[N1qlMetrics](queryResult.info())
      )}
  }
}
