export default interface ISpannerConfig {
  GCPProjectID: string
  SpannerInstanceID: string
  IsMetadataDbCreated?: boolean
  IsConfigValid?: boolean
}
