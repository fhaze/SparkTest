package jp.fhaze.validator

object ValidatorFactory {
  def create(name: String) = name match {
    case "sex" => SexValidator
    case "id"  => IdValidator
    case _     => DummyValidator
  }
}
