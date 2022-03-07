import { Component, Input, OnInit } from '@angular/core'
import { DataService } from 'src/app/services/data/data.service'
import IConv from '../../model/Conv'

@Component({
  selector: 'app-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
})
export class WorkspaceComponent implements OnInit {
  constructor(private data: DataService) {}

  ngOnInit(): void {
    this.data.getSchemaConversionData()
  }
}
