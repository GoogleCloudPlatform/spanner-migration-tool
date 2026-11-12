import { NO_ERRORS_SCHEMA } from '@angular/core';
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { InputType, StorageKeys } from 'src/app/app.constants';
import { DataService } from 'src/app/services/data/data.service';

import { ShardedBulkSourceDetailsFormComponent } from './sharded-bulk-source-details-form.component';

describe('ShardedBulkSourceDetailsFormComponent', () => {
  let component: ShardedBulkSourceDetailsFormComponent;
  let fixture: ComponentFixture<ShardedBulkSourceDetailsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA],
    declarations: [ShardedBulkSourceDetailsFormComponent],
    imports: [MatSnackBarModule],
    providers: [
        {
            provide: MatDialogRef,
            useValue: {
                close: () => { },
            },
        },
        {
            provide: MAT_DIALOG_DATA,
            useValue: {}
        },
        provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting()
    ]
})
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ShardedBulkSourceDetailsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should populate password from DataService when inputType is DirectConnect', () => {
    const dataService = TestBed.inject(DataService);
    dataService.setDirectConnectPassword('session-secret');
    localStorage.setItem(StorageKeys.Type, InputType.DirectConnect);
    localStorage.setItem(
      StorageKeys.Config,
      JSON.stringify({
        dbEngine: 'mysql',
        hostName: '127.0.0.1',
        port: '3306',
        userName: 'admin',
        dbName: 'mydb',
      })
    );

    const newFixture = TestBed.createComponent(ShardedBulkSourceDetailsFormComponent);
    const newComponent = newFixture.componentInstance;

    expect(newComponent.directConnectForm.get('hostName')?.value).toEqual('127.0.0.1');
    expect(newComponent.directConnectForm.get('password')?.value).toEqual('session-secret');

    localStorage.clear();
    dataService.resetStore();
  });
});
