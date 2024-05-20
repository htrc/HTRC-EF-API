package repo

import com.google.inject.ImplementedBy
import play.api.libs.json.JsObject
import repo.models.{IdSet, PageSet, VolumeId, Workset, WorksetId}

import scala.concurrent.Future

@ImplementedBy(classOf[EfRepositoryMongoImpl])
trait EfRepository {
  def hasVolume(id: VolumeId): Future[Boolean]
  def getVolume(id: VolumeId, withPos: Boolean = true, fields: List[String] = List.empty): Future[JsObject]
  def getVolumes(ids: IdSet, withPos: Boolean = true, fields: List[String] = List.empty): Future[List[JsObject]]
  def getVolumesAggregated(ids: IdSet, withPos: Boolean = false, fields: List[String] = List.empty): Future[List[JsObject]]
  def getVolumePages(id: VolumeId, pageSeqs: Option[PageSet] = None, withPos: Boolean = true, fields: List[String] = List.empty): Future[JsObject]

  def getVolumeMetadata(id: VolumeId, fields: List[String] = List.empty): Future[JsObject]
  def getVolumesMetadata(ids: IdSet, fields: List[String] = List.empty): Future[List[JsObject]]

  def createWorkset(ids: IdSet): Future[Workset]
  def deleteWorkset(id: WorksetId): Future[Unit]
  def getWorkset(id: WorksetId): Future[Workset]
}
