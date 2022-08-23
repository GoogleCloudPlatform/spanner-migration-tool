import { NgModule } from '@angular/core'
import { RouterModule, Routes } from '@angular/router'
import { DirectConnectionComponent } from './components/direct-connection/direct-connection.component'
import { HomeComponent } from './components/home/home.component'
import { InstructionComponent } from './components/instruction/instruction.component'
import { LoadDumpComponent } from './components/load-dump/load-dump.component'
import { LoadSessionComponent } from './components/load-session/load-session.component'
import { SourceSelectionComponent } from './components/source-selection/source-selection.component'
import { SummaryComponent } from './components/summary/summary.component'
import { WorkspaceComponent } from './components/workspace/workspace.component'
import { PrepareMigrationComponent } from './components/prepare-migration/prepare-migration.component'

const routes: Routes = [
  {
    path: '',
    component: HomeComponent,
  },
  {
    path: 'source',
    component: SourceSelectionComponent,
    children: [
      {
        path: '',
        redirectTo : "/direct-connection",
        pathMatch: 'full' 
      },
      {
        path: 'direct-connection',
        component: DirectConnectionComponent,
      },
      {
        path: 'load-dump',
        component: LoadDumpComponent,
      },
      {
        path: 'load-session',
        component: LoadSessionComponent,
      },
    ],
  },
  {
    path: 'workspace',
    component: WorkspaceComponent,
  },
  {
    path: 'instruction',
    component: InstructionComponent,
  },
  {
    path: 'prepare-migration',
    component: PrepareMigrationComponent,
  },
  {
    path: '**',
    redirectTo : "/",
    pathMatch: 'full' 
  },
]

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}
