import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { AppComponent } from './app.component';
import { DirectConnectionComponent } from './components/direct-connection/direct-connection.component';
import { HomeComponent } from './components/home/home.component';
import { InstructionComponent } from './components/instruction/instruction.component';
import { LoadDumpComponent } from './components/load-dump/load-dump.component';
import { LoadSessionComponent } from './components/load-session/load-session.component';
import { SourceSelectionComponent } from './components/source-selection/source-selection.component';
import { WorkspaceComponent } from './components/workspace/workspace.component';

const routes: Routes = [
  {
    path: '',
    component: HomeComponent
  },
  {
    path: 'home',
    component: HomeComponent
  },
  {
    path: 'source',
    component: SourceSelectionComponent,
    children: [
      {
        path: 'direct-connection',
        component: DirectConnectionComponent
      },
      {
        path: 'load-dump-file',
        component: DirectConnectionComponent
      },
      {
        path: 'load-session-file',
        component: DirectConnectionComponent
      }
    ]
  },
  {
    path: 'workspace',
    component: WorkspaceComponent,
    children: [],
  },
  {
    path: 'instruction',
    component: InstructionComponent,
    children: [],
  },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
