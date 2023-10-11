import { Component, Inject, OnInit } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';

@Component({
  selector: 'app-equivalent-gcloud-command',
  templateUrl: './equivalent-gcloud-command.component.html',
  styleUrls: ['./equivalent-gcloud-command.component.scss']
})
export class EquivalentGcloudCommandComponent implements OnInit {
  gcloudCmd: string

  constructor(
    @Inject(MAT_DIALOG_DATA) public data: string,
    private dialofRef: MatDialogRef<EquivalentGcloudCommandComponent>
  ) {
    this.gcloudCmd = data
  }

  ngOnInit(): void {
  }
}
