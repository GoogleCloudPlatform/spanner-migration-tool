import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

// Used material modules
import {MatGridListModule} from '@angular/material/grid-list';
import {MatToolbarModule} from '@angular/material/toolbar';
import {MatIconModule} from '@angular/material/icon';
import {MatButtonModule} from '@angular/material/button';
import { HomeComponent } from './components/home/home.component';
import { InstructionComponent } from './components/instruction/instruction.component';
import { ToolbarComponent } from './components/toolbar/toolbar.component';
import { SourceSelectionComponent } from './components/source-selection/source-selection.component';
import { WorkspaceComponent } from './components/workspace/workspace.component';
import { SummaryComponent } from './components/summary/summary.component';
import { DirectConnectionComponent } from './components/direct-connection/direct-connection.component';
import { LoadSessionComponent } from './components/load-session/load-session.component';
import { LoadDumpComponent } from './components/load-dump/load-dump.component';
import { ReportComponent } from './components/report/report.component';

@NgModule({
  declarations: [
    AppComponent,
    HomeComponent,
    InstructionComponent,
    ToolbarComponent,
    SourceSelectionComponent,
    WorkspaceComponent,
    SummaryComponent,
    DirectConnectionComponent,
    LoadSessionComponent,
    LoadDumpComponent,
    ReportComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,

    MatGridListModule,
    MatToolbarModule,
    MatIconModule,
    MatButtonModule
  ],
  providers: [],
  bootstrap: [
    AppComponent
  ]
})
export class AppModule { }
