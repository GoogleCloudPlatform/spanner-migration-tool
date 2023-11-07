import { ISpannerDetails } from "./target-details"


export default interface IDumpConfig {
  Driver: string| null | undefined
  Path: string | null | undefined
}

export interface IConvertFromDumpRequest {
  Config: IDumpConfig
  SpannerDetails: ISpannerDetails
}