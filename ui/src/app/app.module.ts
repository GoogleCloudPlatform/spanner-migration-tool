import { NgModule } from '@angular/core'
import { BrowserModule } from '@angular/platform-browser'

import { AppRoutingModule } from './app-routing.module'
import { AppComponent } from './app.component'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { HomeComponent } from './components/home/home.component'
import { InstructionComponent } from './components/instruction/instruction.component'
import { SourceSelectionComponent } from './components/source-selection/source-selection.component'
import { WorkspaceComponent } from './components/workspace/workspace.component'
import { SummaryComponent } from './components/summary/summary.component'
import { DirectConnectionComponent } from './components/direct-connection/direct-connection.component'
import { LoadSessionComponent } from './components/load-session/load-session.component'
import { LoadDumpComponent } from './components/load-dump/load-dump.component'
import { ReportComponent } from './components/report/report.component'
import { ObjectExplorerComponent } from './components/object-explorer/object-explorer.component'
import { ObjectDetailComponent } from './components/object-detail/object-detail.component'
import { HeaderComponent } from './components/header/header.component'
import { MaterialModule } from './material/material.module'

@NgModule({
  declarations: [
    AppComponent,
    HomeComponent,
    InstructionComponent,
    SourceSelectionComponent,
    WorkspaceComponent,
    SummaryComponent,
    DirectConnectionComponent,
    LoadSessionComponent,
    LoadDumpComponent,
    ReportComponent,
    ObjectExplorerComponent,
    ObjectDetailComponent,
    HeaderComponent,
  ],
  imports: [BrowserModule, AppRoutingModule, BrowserAnimationsModule, MaterialModule],
  providers: [],
  bootstrap: [AppComponent],
})
export class AppModule {}
