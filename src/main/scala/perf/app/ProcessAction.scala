package perf.app

trait ProcessAction

case object ESTIMATE_RDD_SIZE extends ProcessAction
case object EXECUTE extends ProcessAction