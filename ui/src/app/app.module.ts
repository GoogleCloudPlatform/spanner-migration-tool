import { NgModule } from '@angular/core'
import { BrowserModule } from '@angular/platform-browser'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { MaterialModule } from './material/material.module'
import { AppRoutingModule } from './app-routing.module'
import { HttpClientModule, HTTP_INTERCEPTORS } from '@angular/common/http'
import { FormsModule } from '@angular/forms'

import { AppComponent } from './app.component'
import { HeaderComponent } from './components/header/header.component'
import { HomeComponent } from './components/home/home.component'
import { InstructionComponent } from './components/instruction/instruction.component'
import { SourceSelectionComponent } from './components/source-selection/source-selection.component'
import { DirectConnectionComponent } from './components/direct-connection/direct-connection.component'
import { LoadSessionComponent } from './components/load-session/load-session.component'
import { LoadDumpComponent } from './components/load-dump/load-dump.component'
import { WorkspaceComponent } from './components/workspace/workspace.component'
import { ObjectExplorerComponent } from './components/object-explorer/object-explorer.component'
import { ObjectDetailComponent } from './components/object-detail/object-detail.component'
import { SummaryComponent } from './components/summary/summary.component'
import { ReportComponent } from './components/report/report.component'
import { SessionListingComponent } from './components/session-listing/session-listing.component'
import { LoaderComponent } from './components/loader/loader.component'
import { InfodialogComponent } from './components/infodialog/infodialog.component'
import { RuleComponent } from './components/rule/rule.component'
import { InterceptorService } from './services/interceptor/interceptor.service';
import { SaveSessionFormComponent } from './components/save-session-form/save-session-form.component';
import { UpdateSpannerConfigFormComponent } from './components/update-spanner-config-form/update-spanner-config-form.component';
import { SidenavRuleComponent } from './components/sidenav-rule/sidenav-rule.component'

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
    SessionListingComponent,
    LoaderComponent,
    InfodialogComponent,
    RuleComponent,
    SaveSessionFormComponent,
    UpdateSpannerConfigFormComponent,
    SidenavRuleComponent,
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    MaterialModule,
    HttpClientModule,
    FormsModule,
  ],
  providers: [
    {
      provide: HTTP_INTERCEPTORS,
      useClass: InterceptorService,
      multi: true,
    },
  ],
  bootstrap: [AppComponent],
})
export class AppModule {}
