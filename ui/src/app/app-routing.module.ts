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
        path: 'direct-connection',
        component: DirectConnectionComponent,
      },
      {
        path: 'load-dump-file',
        component: LoadDumpComponent,
      },
      {
        path: 'load-session-file',
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
]

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}
