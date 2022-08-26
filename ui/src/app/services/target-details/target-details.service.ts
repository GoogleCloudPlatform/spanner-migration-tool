import { Injectable } from '@angular/core';
import ITargetDetails from 'src/app/model/target-details';

@Injectable({
  providedIn: 'root'
})
export class TargetDetailsService {
  targetDetail: ITargetDetails = { TargetDB: "", Dialect: "google_standard_sql", StreamingConfig: "" };
  constructor() { }
  updateTargetDetails(details: ITargetDetails) {
    this.targetDetail.TargetDB = details.TargetDB;
    this.targetDetail.Dialect = details.Dialect;
    this.targetDetail.StreamingConfig = details.StreamingConfig;
    if (this.targetDetail.StreamingConfig === undefined) {
      this.targetDetail.StreamingConfig = ""
    }
  }
  getTargetDetails() {
    return this.targetDetail;
  }
}