import { ISpannerDetails } from "./target-details"


export default interface IDumpConfig {
  Driver: string
  Path: string
}

export interface IConvertFromDumpRequest {
  Config: IDumpConfig
  SpannerDetails: ISpannerDetails
}