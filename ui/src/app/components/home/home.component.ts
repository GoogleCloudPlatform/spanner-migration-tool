import { Component, OnInit } from '@angular/core'
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { MigrationDetails } from 'src/app/app.constants';
import { InfodialogComponent } from '../infodialog/infodialog.component';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss'],
})
export class HomeComponent implements OnInit {
  constructor(private dialog: MatDialog,
    private router: Router) { }

  ngOnInit(): void {
    if (localStorage.getItem(MigrationDetails.IsMigrationInProgress) != null && localStorage.getItem(MigrationDetails.IsMigrationInProgress) as string === 'true') {
      this.dialog.open(InfodialogComponent, {
        data: { title: 'Redirecting to prepare migration page', message: 'Another migration already in progress', type: 'error' },
        maxWidth: '500px',
      })
      this.router.navigate(['/prepare-migration'])
    }
  }
}
