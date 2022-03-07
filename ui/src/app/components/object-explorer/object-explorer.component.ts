import { Component, OnInit } from '@angular/core'
import IConv from 'src/app/model/Conv'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'
import { DataService } from '../../services/data/data.service'
import { ConversionService } from '../../services/conversion/conversion.service'
import { map } from 'rxjs'
import { FormGroup, FormControl, Validators } from '@angular/forms'

@Component({
  selector: 'app-object-explorer',
  templateUrl: './object-explorer.component.html',
  styleUrls: ['./object-explorer.component.scss'],
})
export class ObjectExplorerComponent implements OnInit {
  // searchForm!: FormGroup
  searchText!: string
  constructor(private data: DataService, private convert: ConversionService) {}
  treeData!: ISchemaObjectNode[]
  convObject!: IConv
  ngOnInit(): void {
    this.data.conv.subscribe((data: IConv) => {
      this.convObject = data
      this.treeData = this.convert.getTableNamesForSidebar(this.convObject, '')
    })
  }

  filterTable(text: any) {
    console.log(text)

    this.treeData = this.convert.getTableNamesForSidebar(this.convObject, text)
  }
}
