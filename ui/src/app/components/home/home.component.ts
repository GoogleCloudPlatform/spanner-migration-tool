import { Component, OnInit } from '@angular/core'
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { MigrationDetails } from 'src/app/app.constants';
import { BackendHealthService } from 'src/app/services/backend-health/backend-health.service';
import { InfodialogComponent } from '../infodialog/infodialog.component';
import { DataService } from 'src/app/services/data/data.service';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss'],
})
export class HomeComponent implements OnInit {
  isOfflineStatus: boolean = false

  constructor(private dialog: MatDialog,
    private router: Router,
    private data: DataService,
    private healthCheckService: BackendHealthService) {

     }

  ngOnInit(): void {
    this.data.isOffline.subscribe({
      next: (res: boolean) => {
        this.isOfflineStatus = res
      },
    })

    this.healthCheckService.startHealthCheck();
    if (localStorage.getItem(MigrationDetails.IsMigrationInProgress) != null && localStorage.getItem(MigrationDetails.IsMigrationInProgress) as string === 'true') {
      this.dialog.open(InfodialogComponent, {
        data: { title: 'Redirecting to prepare migration page', message: 'Another migration already in progress', type: 'error' },
        maxWidth: '500px',
      })
      this.router.navigate(['/prepare-migration'])
    }
  }

  connectToDatabase(){
    if(this.isOfflineStatus){
      this.dialog.open(InfodialogComponent, {
        data: { message: "Please configure spanner project id and instance id to proceed", type: 'error', title: 'Configure Spanner' },
        maxWidth: '500px',
      })
    }
    else{
      this.router.navigate(['/source/direct-connection'])
    }
  }
}
