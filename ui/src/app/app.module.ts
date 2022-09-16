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
import { InterceptorService } from './services/interceptor/interceptor.service'
import { UpdateSpannerConfigFormComponent } from './components/update-spanner-config-form/update-spanner-config-form.component'
import { SidenavRuleComponent } from './components/sidenav-rule/sidenav-rule.component'
import { AddIndexFormComponent } from './components/add-index-form/add-index-form.component'
import { EditGlobalDatatypeFormComponent } from './components/edit-global-datatype-form/edit-global-datatype-form.component'
import { SidenavViewAssessmentComponent } from './components/sidenav-view-assessment/sidenav-view-assessment.component'
import { SidenavSaveSessionComponent } from './components/sidenav-save-session/sidenav-save-session.component'
import { DropIndexOrTableDialogComponent } from './components/drop-index-or-table-dialog/drop-index-or-table-dialog.component'
import { DatabaseLoaderComponent } from './components/database-loader/database-loader.component'
import { SidenavReviewChangesComponent } from './components/sidenav-review-changes/sidenav-review-changes.component'
import { TableColumnChangesPreviewComponent } from './components/table-column-changes-preview/table-column-changes-preview.component'
import { PrepareMigrationComponent } from './components/prepare-migration/prepare-migration.component'
import { TargetDetailsFormComponent } from './components/target-details-form/target-details-form.component'

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
    UpdateSpannerConfigFormComponent,
    SidenavRuleComponent,
    AddIndexFormComponent,
    EditGlobalDatatypeFormComponent,
    SidenavViewAssessmentComponent,
    SidenavSaveSessionComponent,
    DropIndexOrTableDialogComponent,
    DatabaseLoaderComponent,
    SidenavReviewChangesComponent,
    TableColumnChangesPreviewComponent,
    PrepareMigrationComponent,
    TargetDetailsFormComponent,
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
