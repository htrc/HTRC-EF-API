package controllers

import io.swagger.annotations.ApiParam
import play.api.mvc._
import protocol.WrappedResponse
import repo.EfRepository
import repo.models.{VolumeId, WorksetId}
import utils.Helper.tokenize
import utils.IdUtils._

import javax.inject.Inject
import scala.concurrent.ExecutionContext

class EfController @Inject()(efRepository: EfRepository,
                             components: ControllerComponents)
                            (implicit val ec: ExecutionContext) extends AbstractController(components) {
  def getVolume(@ApiParam(value = "the 'clean' HTID of the volume", required = true) cleanId: VolumeId,
                @ApiParam(value = "'true' whether to include part-of-speech information for tokens, " +
                  "'false' otherwise", required = false, defaultValue = "true") pos: Boolean,
                @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val id = uncleanId(cleanId)
          efRepository.getVolume(id, pos, fields.map(tokenize(_)).getOrElse(List.empty)).map(WrappedResponse(_))
      }
    }

  def checkVolume(@ApiParam(value = "the 'clean' HTID of the volume", required = true) cleanId: VolumeId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val id = uncleanId(cleanId)
          efRepository.hasVolume(id).map {
            case true => Ok
            case _ => NotFound
          }
      }
    }

  def getVolumeMetadata(@ApiParam(value = "the 'clean' HTID of the volume", required = true) cleanId: VolumeId,
                        @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val id = uncleanId(cleanId)
          efRepository.getVolumeMetadata(id, fields.map(tokenize(_)).getOrElse(List.empty)).map(WrappedResponse(_))
      }
    }

  def getVolumePages(@ApiParam(value = "the 'clean' HTID of the volume", required = true) cleanId: VolumeId,
                     @ApiParam(value = "comma-separated list of page sequence numbers to fetch") seq: Option[String],
                     @ApiParam(value = "'true' whether to include part-of-speech information for tokens, " +
                       "'false' otherwise", required = false, defaultValue = "true") pos: Boolean,
                     @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          val id = uncleanId(cleanId)
          val seqs = seq.map(_.split(',').toSet)
          efRepository.getVolumePages(id, seqs, pos, fields.map(tokenize(_)).getOrElse(List.empty)).map(WrappedResponse(_))
      }
    }

  def createWorkset(): Action[String] =
    Action.async(parse.text) { implicit req =>
      render.async {
        case Accepts.Json() =>
          val ids = tokenize(req.body, delims = " \n").toSet
          efRepository.createWorkset(ids).map(WrappedResponse(_))
      }
    }

  def getWorkset(@ApiParam(value = "the workset ID", required = true) wid: WorksetId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository
            .getWorkset(wid)
            .map(WrappedResponse(_))
      }
    }

  def deleteWorkset(@ApiParam(value = "the workset ID", required = true) wid: WorksetId): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository.deleteWorkset(wid).map(_ => WrappedResponse.Empty)
      }
    }

  def getWorksetVolumes(@ApiParam(value = "the workset ID", required = true) wid: WorksetId,
                        @ApiParam(value = "'true' whether to include part-of-speech information for tokens, 'false' otherwise", required = false, defaultValue = "true") pos: Boolean,
                        @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository
            .getWorkset(wid)
            .flatMap(workset => efRepository.getVolumes(workset.htids, pos, fields.map(tokenize(_)).getOrElse(List.empty)))
            .map(WrappedResponse(_))
      }
    }

  def getWorksetVolumesAggregated(@ApiParam(value = "the workset ID", required = true) wid: WorksetId,
                                  @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] = {
    //TODO: Add query parameter to specify whether to include POS information if that data becomes available in the future
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository
            .getWorkset(wid)
            .flatMap(workset => efRepository.getVolumesAggregated(workset.htids, withPos = false, fields.map(tokenize(_)).getOrElse(List.empty)))
            .map(WrappedResponse(_))
      }
    }
  }

  def getWorksetVolumesMetadata(@ApiParam(value = "the workset ID", required = true) wid: WorksetId,
                                @ApiParam(value = "comma-separated list of fields to return") fields: Option[String]): Action[AnyContent] =
    Action.async { implicit req =>
      render.async {
        case Accepts.Json() =>
          efRepository
            .getWorkset(wid)
            .flatMap(workset => efRepository.getVolumesMetadata(workset.htids, fields.map(tokenize(_)).getOrElse(List.empty)))
            .map(WrappedResponse(_))
      }
    }
}
