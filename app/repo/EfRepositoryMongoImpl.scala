package repo

import akka.stream.Materializer
import exceptions.{VolumeNotFoundException, WorksetNotFoundException}
import play.api.Logging
import play.api.libs.json._
import reactivemongo.api.bson._
import repo.models._

import java.time.Instant
import javax.inject.{Inject, Singleton}
import scala.collection.mutable

// Reactive Mongo imports
import play.modules.reactivemongo.{ReactiveMongoApi, ReactiveMongoComponents}
import reactivemongo.api.bson.collection.BSONCollection

// BSON-JSON conversions/collection
import reactivemongo.akkastream.cursorProducer
import reactivemongo.play.json.compat.json2bson.toDocumentReader

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EfRepositoryMongoImpl @Inject()(val reactiveMongoApi: ReactiveMongoApi)
                                     (implicit ec: ExecutionContext, m: Materializer)
  extends EfRepository with ReactiveMongoComponents with Logging {

  protected def efCol: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("ef"))
  protected def featuresCol: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("features"))
  protected def metadataCol: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("metadata"))
  protected def pagesCol: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("pages"))
  protected def worksetsCol: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("worksets"))


  override def hasVolume(id: VolumeId): Future[Boolean] = {
    val query = document("htid" -> id)

    efCol
      .map(_.count(Some(query), limit = Some(1)))
      .flatMap(_.map(_ == 1))
  }

  override def getVolume(id: VolumeId, withPos: Boolean = true, fields: List[String] = List.empty): Future[JsObject] =
    if (withPos) getVolumeWithPos(id, fields) else getVolumeNoPos(id, fields)

  override def getVolumes(ids: IdSet, withPos: Boolean = true, fields: List[String] = List.empty): Future[List[JsObject]] =
    if (withPos) getVolumesWithPos(ids, fields) else getVolumesNoPos(ids, fields)

  protected def getVolumeWithPos(id: VolumeId, fields: List[String] = List.empty): Future[JsObject] =
    getVolumesWithPos(Set(id), fields).map {
      case vol :: Nil => vol
      case _ => throw VolumeNotFoundException(id)
    }

  protected def getVolumesWithPos(ids: IdSet, fields: List[String] = List.empty): Future[List[JsObject]] = {
    val newFields = fields.map(f => f.replaceAll("features\\.pages", "page"))
    val projFields =
      if (fields.nonEmpty) Some(BSONDocument(newFields.map(_ -> BSONInteger(1))) ++ ("htid" -> BSONInteger(1)))
      else None

    for {
      efData <- efCol.map(_.find(document("htid" -> document("$in" -> ids)), projFields).cursor[JsObject]()).flatMap(_.collect[List]())
      featuresData <- featuresCol.map(_.find(document("htid" -> document("$in" -> ids)), projFields).cursor[JsObject]()).flatMap(_.collect[List]())
      metaData <- metadataCol.map(_.find(document("htid" -> document("$in" -> ids)), projFields).cursor[JsObject]()).flatMap(_.collect[List]())
      pagesData <- pagesCol.map(_.find(document("htid" -> document("$in" -> ids)), projFields).cursor[JsObject]()).flatMap(_.collect[List]())
    }
      yield {
        val efDataMap = efData.map(o => (o \ "htid").as[String] -> o).toMap
        val featuresDataMap = featuresData.map(o => (o \ "htid").as[String] -> o).toMap
        val metaDataMap = metaData.map(o => (o \ "htid").as[String] -> o).toMap
        val pagesDataMap = pagesData.map(o => (o \ "htid").as[String] -> o)
          .foldLeft(Map.empty[String, mutable.ListBuffer[JsObject]].withDefaultValue(mutable.ListBuffer.empty)) {
            case (map, (k, v)) => map(k) += v; map
          }
        ids.toList
          .map { htid =>
            val ef = efDataMap(htid)
            var features = featuresDataMap(htid)
            if (!newFields.exists(_.startsWith("features")) && newFields.exists(_.startsWith("page")))
              features = Json.obj("features" -> Json.obj())
            val metadata = metaDataMap(htid)
            val pages = pagesDataMap(htid)
            val thePages =
              if (pages.forall(_.keys.contains("page")))
                Json.obj(
                  "pages" -> pages.map(_ \ "page").map(_.as[JsObject])
                )
              else Json.obj()
            val addPagesTransform = (__ \ "features").json.update(__.json.put(thePages))
            val result = ef ++ metadata - "_id" ++ features.transform(addPagesTransform).get
            if (!fields.contains("htid")) result - "htid"
            else result
          }
      }
  }

    protected def getVolumeNoPos(id: VolumeId, fields: List[String] = List.empty): Future[JsObject] =
    getVolumesNoPos(Set(id), fields).map {
      case vol :: Nil => vol
      case _ => throw VolumeNotFoundException(id)
    }

  //db.getCollection("ef").aggregate([
  //    {
  //        $match: {
  //            htid: { $in: ['hvd.32044019369404'] }
  //        }
  //    },
  //    {
  //        $lookup: {
  //            from: 'metadata',
  //            let: { htid: '$htid' },
  //            pipeline: [
  //                {
  //                    $match: {
  //                        $expr: {
  //                            $eq: ['$htid', '$$htid']
  //                        }
  //                    }
  //                },
  //                { $project: { _id: 0 } },
  //                { $replaceRoot: { newRoot: '$metadata' } }
  //            ],
  //            as: 'metadata'
  //        }
  //    },
  //    {
  //        $unwind: '$metadata'
  //    },
  //    {
  //        $lookup: {
  //            from: 'features',
  //            let: { htid: '$htid' },
  //            pipeline: [
  //                {
  //                    $match: {
  //                        $expr: {
  //                            $eq: ['$htid', '$$htid']
  //                        }
  //                    }
  //                },
  //                { $project: { _id: 0 } },
  //                { $replaceRoot: { newRoot: '$features' } }
  //            ],
  //            as: 'features'
  //        }
  //    },
  //    {
  //        $unwind: '$features'
  //    },
  //    {
  //        $lookup: {
  //            from: 'pages',
  //            let: { htid: '$htid' },
  //            pipeline: [
  //                {
  //                    $match: {
  //                        $expr: {
  //                            $eq: ['$htid', '$$htid']
  //                        }
  //                    }
  //                },
  //                { $project: { _id: 0 } },
  //                {
  //                    $addFields: {
  //                        "page.header.tokenPosCount": {
  //                            $map: {
  //                                input: { $objectToArray: "$page.header.tokenPosCount" },
  //                                as: "htpc",
  //                                in: {
  //                                    k: "$$htpc.k",
  //                                    v: {
  //                                        $objectToArray: "$$htpc.v"
  //                                    }
  //                                }
  //                            }
  //                        },
  //                        "page.body.tokenPosCount": {
  //                            $map: {
  //                                input: { $objectToArray: "$page.body.tokenPosCount" },
  //                                as: "btpc",
  //                                in: {
  //                                    k: "$$btpc.k",
  //                                    v: {
  //                                        $objectToArray: "$$btpc.v"
  //                                    }
  //                                }
  //                            }
  //                        },
  //                        "page.footer.tokenPosCount": {
  //                            $map: {
  //                                input: { $objectToArray: "$page.footer.tokenPosCount" },
  //                                as: "ftpc",
  //                                in: {
  //                                    k: "$$ftpc.k",
  //                                    v: {
  //                                        $objectToArray: "$$ftpc.v"
  //                                    }
  //                                }
  //                            }
  //                        }
  //                    }
  //                },
  //                {
  //                    $addFields: {
  //                        "page.header.tokensCount": {
  //                            $arrayToObject: {
  //                                $map: {
  //                                    input: "$page.header.tokenPosCount",
  //                                    as: "htpc",
  //                                    in: {
  //                                        k: "$$htpc.k",
  //                                        v: {
  //                                            $sum: "$$htpc.v.v"
  //                                        }
  //                                    }
  //                                }
  //                            }
  //                        },
  //                        "page.body.tokensCount": {
  //                            $arrayToObject: {
  //                                $map: {
  //                                    input: "$page.body.tokenPosCount",
  //                                    as: "btpc",
  //                                    in: {
  //                                        k: "$$btpc.k",
  //                                        v: {
  //                                            $sum: "$$btpc.v.v"
  //                                        }
  //                                    }
  //                                }
  //                            }
  //                        },
  //                        "page.footer.tokensCount": {
  //                            $arrayToObject: {
  //                                $map: {
  //                                    input: "$page.footer.tokenPosCount",
  //                                    as: "ftpc",
  //                                    in: {
  //                                        k: "$$ftpc.k",
  //                                        v: {
  //                                            $sum: "$$ftpc.v.v"
  //                                        }
  //                                    }
  //                                }
  //                            }
  //                        }
  //                    }
  //                },
  //                {
  //                    $project: {
  //                        "page.header.tokenPosCount": 0,
  //                        "page.body.tokenPosCount": 0,
  //                        "page.footer.tokenPosCount": 0
  //                    }
  //                },
  //
  //                { $replaceRoot: { newRoot: '$page' } }
  //            ],
  //            as: 'features.pages'
  //        }
  //    },
  //    {
  //        $project: {
  //            _id: 0
  //        }
  //    }
  //])
  protected def getVolumesNoPos(ids: IdSet, fields: List[String] = List.empty): Future[List[JsObject]] = {
    //require(ids.nonEmpty)

    val projFields = BSONDocument(fields.map(f => f -> BSONInteger(1)))

    for {
      col <- efCol; features <- featuresCol; metadata <- metadataCol; pages <- pagesCol
      volumes <- col
        .aggregateWith[JsObject]() { framework =>
          import framework._

          val query = if (ids.isEmpty) document() else document("htid" -> document("$in" -> ids))

          List(
            Match(query),
            LookupPipeline(
              from = metadata.name,
              let = document("htid" -> "$htid"),
              pipeline = List(
                Match(document("$expr" -> document("$eq" -> List("$htid", """$$htid""")))),
                Project(document("_id" -> 0)),
                ReplaceRootField("metadata")
              ),
              as = "metadata"
            ),
            UnwindField("metadata"),
            LookupPipeline(
              from = features.name,
              let = document("htid" -> "$htid"),
              pipeline = List(
                Match(document("$expr" -> document("$eq" -> List("$htid", """$$htid""")))),
                Project(document("_id" -> 0)),
                ReplaceRootField("features")
              ),
              as = "features"
            ),
            UnwindField("features"),
            LookupPipeline(
              from = pages.name,
              let = document("htid" -> "$htid"),
              pipeline = List(
                Match(document("$expr" -> document("$eq" -> List("$htid", """$$htid""")))),
                Project(document("_id" -> 0)),
                AddFields(document(
                  "page.header.tokenPosCount" -> document(
                    "$map" -> document(
                      "input" -> document("$objectToArray" -> "$page.header.tokenPosCount"),
                      "as" -> "htpc",
                      "in" -> document(
                        "k" -> "$$htpc.k",
                        "v" -> document("$objectToArray" -> "$$htpc.v")
                      )
                    )
                  ),
                  "page.body.tokenPosCount" -> document(
                    "$map" -> document(
                      "input" -> document("$objectToArray" -> "$page.body.tokenPosCount"),
                      "as" -> "btpc",
                      "in" -> document(
                        "k" -> "$$btpc.k",
                        "v" -> document("$objectToArray" -> "$$btpc.v")
                      )
                    )
                  ),
                  "page.footer.tokenPosCount" -> document(
                    "$map" -> document(
                      "input" -> document("$objectToArray" -> "$page.footer.tokenPosCount"),
                      "as" -> "ftpc",
                      "in" -> document(
                        "k" -> "$$ftpc.k",
                        "v" -> document("$objectToArray" -> "$$ftpc.v")
                      )
                    )
                  )
                )),
                AddFields(document(
                  "page.header.tokensCount" -> document(
                    "$arrayToObject" -> document(
                      "$map" -> document(
                        "input" -> "$page.header.tokenPosCount",
                        "as" -> "htpc",
                        "in" -> document(
                          "k" -> "$$htpc.k",
                          "v" -> document("$sum" -> "$$htpc.v.v")
                        )
                      )
                    )
                  ),
                  "page.body.tokensCount" -> document(
                    "$arrayToObject" -> document(
                      "$map" -> document(
                        "input" -> "$page.body.tokenPosCount",
                        "as" -> "btpc",
                        "in" -> document(
                          "k" -> "$$btpc.k",
                          "v" -> document("$sum" -> "$$btpc.v.v")
                        )
                      )
                    )
                  ),
                  "page.footer.tokensCount" -> document(
                    "$arrayToObject" -> document(
                      "$map" -> document(
                        "input" -> "$page.footer.tokenPosCount",
                        "as" -> "ftpc",
                        "in" -> document(
                          "k" -> "$$ftpc.k",
                          "v" -> document("$sum" -> "$$ftpc.v.v")
                        )
                      )
                    )
                  )
                )),
                Project(document(
                  "page.header.tokenPosCount" -> 0,
                  "page.body.tokenPosCount" -> 0,
                  "page.footer.tokenPosCount" -> 0
                )),
                ReplaceRootField("page")
              ),
              as = "features.pages"
            ),
            Project(document("_id" -> 0) ++ projFields)
          )
        }
        .collect[List]()
    } yield volumes
  }

  override def getVolumeMetadata(id: VolumeId, fields: List[String] = List.empty): Future[JsObject] = {
    val ids = Set(id)
    getVolumesMetadata(ids, fields).map {
      case meta :: Nil => meta
      case _ => throw VolumeNotFoundException(id)
    }
  }

  override def getVolumesMetadata(ids: IdSet, fields: List[String] = List.empty): Future[List[JsObject]] = {
    //require(ids.nonEmpty)

    val query = if (ids.isEmpty) document() else document("htid" -> document("$in" -> ids))
    val projFields = BSONDocument(fields.map(f => f -> BSONInteger(1)))
    val projection = document("_id" -> 0) ++ projFields

    metadataCol
      .map(_.find(query, Some(projection)))
      .map(_.cursor[JsObject]())
      .flatMap(_.collect[List]())
  }

  override def getVolumePages(id: VolumeId, pageSeqs: Option[PageSet] = None, withPos: Boolean = true, fields: List[String] = List.empty): Future[JsObject] =
    if (withPos) getVolumePagesWithPos(id, pageSeqs, fields) else getVolumePagesNoPos(id, pageSeqs, fields)

  protected def getVolumePagesWithPos(id: VolumeId, pageSeqs: Option[PageSet] = None, fields: List[String] = List.empty): Future[JsObject] = {
    pagesCol.flatMap { col =>
      var query = document("htid" -> id)
      pageSeqs.foreach(seqs => query ++= "page.seq" -> document("$in" -> seqs))

      val projFields = BSONDocument(fields.map(f => f -> BSONInteger(1)))

      col
        .aggregateWith[JsObject]() { framework =>
          import framework._

          List(
            Match(query),
            GroupField("htid")(
              "htid" -> FirstField("htid"),
              "pages" -> PushField("page")
            ),
            Project(document("_id" -> 0) ++ projFields)
          )
        }
        .headOption
        .map(_.getOrElse { throw VolumeNotFoundException(id) })
    }
  }

  //db.getCollection("pages").aggregate([
  //    {
  //        $match: {
  //            htid: 'hvd.32044019369404',
  //            "page.seq": { $in: ['00000119'] }
  //        }
  //    },
  //    {
  //        $addFields: {
  //            "page.header.tokenPosCount": {
  //                $map: {
  //                    input: { $objectToArray: "$page.header.tokenPosCount" },
  //                    as: "htpc",
  //                    in: {
  //                        k: "$$htpc.k",
  //                        v: {
  //                            $objectToArray: "$$htpc.v"
  //                        }
  //                    }
  //                }
  //            },
  //            "page.body.tokenPosCount": {
  //                $map: {
  //                    input: { $objectToArray: "$page.body.tokenPosCount" },
  //                    as: "btpc",
  //                    in: {
  //                        k: "$$btpc.k",
  //                        v: {
  //                            $objectToArray: "$$btpc.v"
  //                        }
  //                    }
  //                }
  //            },
  //            "page.footer.tokenPosCount": {
  //                $map: {
  //                    input: { $objectToArray: "$page.footer.tokenPosCount" },
  //                    as: "ftpc",
  //                    in: {
  //                        k: "$$ftpc.k",
  //                        v: {
  //                            $objectToArray: "$$ftpc.v"
  //                        }
  //                    }
  //                }
  //            }
  //        }
  //    },
  //    {
  //        $addFields: {
  //            "page.header.tokensCount": {
  //                $arrayToObject: {
  //                    $map: {
  //                        input: "$page.header.tokenPosCount",
  //                        as: "htpc",
  //                        in: {
  //                            k: "$$htpc.k",
  //                            v: {
  //                                $sum: "$$htpc.v.v"
  //                            }
  //                        }
  //                    }
  //                }
  //            },
  //            "page.body.tokensCount": {
  //                $arrayToObject: {
  //                    $map: {
  //                        input: "$page.body.tokenPosCount",
  //                        as: "btpc",
  //                        in: {
  //                            k: "$$btpc.k",
  //                            v: {
  //                                $sum: "$$btpc.v.v"
  //                            }
  //                        }
  //                    }
  //                }
  //            },
  //            "page.footer.tokensCount": {
  //                $arrayToObject: {
  //                    $map: {
  //                        input: "$page.footer.tokenPosCount",
  //                        as: "ftpc",
  //                        in: {
  //                            k: "$$ftpc.k",
  //                            v: {
  //                                $sum: "$$ftpc.v.v"
  //                            }
  //                        }
  //                    }
  //                }
  //            }
  //        }
  //    },
  //    {
  //        $project: {
  //            "page.header.tokenPosCount": 0,
  //            "page.body.tokenPosCount": 0,
  //            "page.footer.tokenPosCount": 0
  //        }
  //    },
  //    {
  //        $group: {
  //            _id: "$htid",
  //            htid: { $first: "$htid" },
  //            pages: { $push: "$page" }
  //        }
  //    }
  //])
  protected def getVolumePagesNoPos(id: VolumeId, pageSeqs: Option[PageSet], fields: List[String] = List.empty): Future[JsObject] = {
    pagesCol.flatMap { col =>
      var query = document("htid" -> id)
      pageSeqs.foreach(seqs => query ++= "page.seq" -> document("$in" -> seqs))

      val projFields = BSONDocument(fields.map(f => f -> BSONInteger(1)))

      col
        .aggregateWith[JsObject]() { framework =>
          import framework._

          List(
            Match(query),
            AddFields(document(
              "page.header.tokenPosCount" -> document(
                "$map" -> document(
                  "input" -> document("$objectToArray" -> "$page.header.tokenPosCount"),
                  "as" -> "htpc",
                  "in" -> document(
                    "k" -> "$$htpc.k",
                    "v" -> document("$objectToArray" -> "$$htpc.v")
                  )
                )
              ),
              "page.body.tokenPosCount" -> document(
                "$map" -> document(
                  "input" -> document("$objectToArray" -> "$page.body.tokenPosCount"),
                  "as" -> "btpc",
                  "in" -> document(
                    "k" -> "$$btpc.k",
                    "v" -> document("$objectToArray" -> "$$btpc.v")
                  )
                )
              ),
              "page.footer.tokenPosCount" -> document(
                "$map" -> document(
                  "input" -> document("$objectToArray" -> "$page.footer.tokenPosCount"),
                  "as" -> "ftpc",
                  "in" -> document(
                    "k" -> "$$ftpc.k",
                    "v" -> document("$objectToArray" -> "$$ftpc.v")
                  )
                )
              )
            )),
            AddFields(document(
              "page.header.tokensCount" -> document(
                "$arrayToObject" -> document(
                  "$map" -> document(
                    "input" -> "$page.header.tokenPosCount",
                    "as" -> "htpc",
                    "in" -> document(
                      "k" -> "$$htpc.k",
                      "v" -> document("$sum" -> "$$htpc.v.v")
                    )
                  )
                )
              ),
              "page.body.tokensCount" -> document(
                "$arrayToObject" -> document(
                  "$map" -> document(
                    "input" -> "$page.body.tokenPosCount",
                    "as" -> "btpc",
                    "in" -> document(
                      "k" -> "$$btpc.k",
                      "v" -> document("$sum" -> "$$btpc.v.v")
                    )
                  )
                )
              ),
              "page.footer.tokensCount" -> document(
                "$arrayToObject" -> document(
                  "$map" -> document(
                    "input" -> "$page.footer.tokenPosCount",
                    "as" -> "ftpc",
                    "in" -> document(
                      "k" -> "$$ftpc.k",
                      "v" -> document("$sum" -> "$$ftpc.v.v")
                    )
                  )
                )
              )
            )),
            Project(document(
              "page.header.tokenPosCount" -> 0,
              "page.body.tokenPosCount" -> 0,
              "page.footer.tokenPosCount" -> 0
            )),
            GroupField("htid")(
              "htid" -> FirstField("htid"),
              "pages" -> PushField("page")
            ),
            Project(document("_id" -> 0) ++ projFields)
          )
        }
        .headOption
        .map(_.getOrElse { throw VolumeNotFoundException(id) })
    }
  }

  // db.getCollection("ef").aggregate([
  //    {
  //        $match: {
  //            htid: { $in: ["hvd.32044103226122", "hvd.32044090301284", "abc.12345123"] }
  //        }
  //    },
  //    {
  //        $group: {
  //            _id: null,
  //            htids: { $push: "$htid" }
  //        }
  //    },
  //    {
  //        $project: {
  //            _id: 0
  //        }
  //    },
  //    {
  //        $addFields: {
  //            created: "$$NOW"
  //        }
  //    },
  //    {
  //        $merge: {
  //            into: "worksets"
  //        }
  //    }
  // ])
  override def createWorkset(ids: IdSet): Future[Workset] = {
    require(ids.nonEmpty)

    val worksetId = BSONObjectID.generate()

    efCol
      .flatMap(_
        .aggregateWith[Workset]() { framework =>
          import framework._

          List(
            Match(document("htid" -> document("$in" -> ids))),
            Group(BSONNull)("htids" -> PushField("htid")),
            AddFields(document(
              "_id" -> worksetId,
              "created" -> Instant.now
            )),
            Merge(intoCollection = "worksets", on = List("_id"), None, None, None)
          )
        }
        .headOption
      )
      .flatMap(_ => getWorkset(worksetId.stringify))
  }

  override def deleteWorkset(id: WorksetId): Future[Unit] = {
    val worksetId = BSONObjectID.parse(id).get

    worksetsCol
      .flatMap(_.delete().one(document("_id" -> worksetId)))
      .map(_ => ())
  }

  override def getWorkset(id: WorksetId): Future[Workset] = {
    worksetsCol
      .flatMap(_
        .find(document("_id" -> BSONObjectID.parse(id).get))
        .one[Workset]
        .map {
          case Some(workset) => workset
          case None => throw WorksetNotFoundException(id)
        }
      )
  }
}
