export default interface ISequence {
    Name: string
    SequenceKind: string
    SkipRangeMin?: number
    SkipRangeMax?: number
    StartWithCounter?: number
    ColumnsUsingSeq?: Map<string, string[]>
  }
