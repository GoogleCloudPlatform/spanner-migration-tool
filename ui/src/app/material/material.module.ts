import { NgModule } from '@angular/core'
import { MatGridListModule } from '@angular/material/grid-list'
import { MatToolbarModule } from '@angular/material/toolbar'
import { MatButtonModule } from '@angular/material/button'
import { MatIconModule } from '@angular/material/icon'

const materialModules = [MatGridListModule, MatToolbarModule, MatButtonModule, MatIconModule]
@NgModule({
  declarations: [],
  imports: materialModules,
  exports: materialModules,
})
export class MaterialModule {}
