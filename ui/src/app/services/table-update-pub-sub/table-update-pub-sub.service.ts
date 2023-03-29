import { Injectable } from '@angular/core'
import { BehaviorSubject } from 'rxjs'
import IUpdateTable, { IReviewUpdateTable } from 'src/app/model/update-table'

@Injectable({
  providedIn: 'root',
})
export class TableUpdatePubSubService {
  private reviewTableChangesSub = new BehaviorSubject<IReviewUpdateTable>({ Changes: [], DDL: '' })
  private tableUpdateDetailSub = new BehaviorSubject<{
    tableName: string
    tableId: string
    updateDetail: IUpdateTable
  }>({ tableName: '', tableId: '', updateDetail: { UpdateCols: {} } })

  constructor() {}
  reviewTableChanges = this.reviewTableChangesSub.asObservable()
  tableUpdateDetail = this.tableUpdateDetailSub.asObservable()

  setTableReviewChanges(data: IReviewUpdateTable) {
    this.reviewTableChangesSub.next(data)
  }
  setTableUpdateDetail(data: { tableName: string; tableId: string; updateDetail: IUpdateTable }) {
    this.tableUpdateDetailSub.next(data)
  }
}
