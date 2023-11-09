import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { catchError, from, interval, map, Observable, of, Subject, Subscription, takeUntil } from 'rxjs';
import { InfodialogComponent } from 'src/app/components/infodialog/infodialog.component';
import { FetchService } from '../fetch/fetch.service';

@Injectable({
  providedIn: 'root'
})
export class BackendHealthService {
  private healthCheckSubscription: Subscription = new Subscription;
  private unHealthyCheckCount: number = 0;

  constructor(private fetch: FetchService,
    private dialog: MatDialog) { }

    startHealthCheck() {
      this.healthCheckSubscription = interval(5000).subscribe(() => {
        this.checkBackendHealth();
      });
    }
  
    stopHealthCheck() {
      if (this.healthCheckSubscription) {
        this.healthCheckSubscription.unsubscribe();
      }
    }
  
    checkBackendHealth() {
      this.checkHealth().subscribe(
        (isHealthy) => {
          if (!isHealthy) {
            if (this.unHealthyCheckCount == 5) {
              // Backend is unhealthy, open the dialog and unsubscribe
              this.openHealthDialog();
            }
            this.unHealthyCheckCount++;
          } else {
            this.unHealthyCheckCount = 0;
          }
        }
      );
    }

  openHealthDialog() {
    let dialogRef = this.dialog.open(InfodialogComponent, {
      width: '500px',
      data: {
        message: "Please check terminal logs for more details. In case of a crash please file a <a href='https://github.com/GoogleCloudPlatform/spanner-migration-tool/issues' target='_blank' class='a-link'>github</a> issue with all the details.",
        type: 'error',
        title: 'Backend server unresponsive',
      }
    });
    this.stopHealthCheck();
    dialogRef.afterClosed().subscribe(() => {
      this.startHealthCheck();
    })
  }

  checkHealth(): Observable<boolean> {
    return from(this.fetch.checkBackendHealth()).pipe(
      map(() => true),
      catchError(() => {
        return of(false);
      })
    );
  }

  ngOnDestroy() {
    // Stop health checks when the service is destroyed
    this.stopHealthCheck();
  }
}
