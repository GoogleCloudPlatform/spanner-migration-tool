import ConversionRate from './conversion-rate'

export default interface IViewAssesmentData {
  srcDbType: string
  connectionDetail: string
  conversionRates: ConversionRate
}
