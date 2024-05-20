package exceptions

case class VolumeNotFoundException(id: String, cause: Throwable = null)
  extends ResourceNotFoundException(s"Volume $id not found", cause)

case class NotImplementedException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause)