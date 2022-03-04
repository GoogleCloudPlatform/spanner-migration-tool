import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { AppComponent } from './app.component';
import { DirectConnectionComponent } from './direct-connection/direct-connection.component';
import { HomeComponent } from './home/home.component';
import { InstructionComponent } from './instruction/instruction.component';
import { LoadDumpComponent } from './load-dump/load-dump.component';
import { LoadSessionComponent } from './load-session/load-session.component';
import { SourceSelectionComponent } from './source-selection/source-selection.component';
import { WorkspaceComponent } from './workspace/workspace.component';

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
    component: SourceSelectionComponent
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
