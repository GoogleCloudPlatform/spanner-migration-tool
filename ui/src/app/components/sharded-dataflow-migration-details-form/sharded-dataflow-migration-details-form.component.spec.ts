import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormBuilder, ReactiveFormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { ShardedDataflowMigrationDetailsFormComponent } from './sharded-dataflow-migration-details-form.component';import { FetchService } from 'src/app/services/fetch/fetch.service';
import { of, throwError } from 'rxjs';
import IConnectionProfile, { IMigrationProfile} from 'src/app/model/profile'
import IConv from 'src/app/model/conv';
import { DataService } from 'src/app/services/data/data.service';
import { error } from 'console';

describe('ShardedDataflowMigrationDetailsFormComponent', () => {
  let component: ShardedDataflowMigrationDetailsFormComponent;
  let fixture: ComponentFixture<ShardedDataflowMigrationDetailsFormComponent>;
  let fetchServiceSpy: jasmine.SpyObj<FetchService>;
  let dataServiceSpy: jasmine.SpyObj<DataService>;
  const mockMigrationProfile:IMigrationProfile = {
    configType: 'value',
    shardConfigurationDataflow: {
      schemaSource: {
        host: '',
        user: '',
        password: '',
        port: '',
        dbName: ''
      },
      dataShards: []
    }
  }
  const mockConnectionProfiles: IConnectionProfile[] = [
    { 
      DisplayName: 'name',
      Name: 'name'
   },];
   const mockStaticIps = ['192.168.1.10', '10.0.0.5'];

  beforeEach(async () => {
    fetchServiceSpy = jasmine.createSpyObj('FetchService', ['verifyJsonConfiguration', 'getLastSessionDetails', 'getConnectionProfiles', 'getStaticIps']);
    dataServiceSpy = jasmine.createSpyObj('DataService', ['getLastSessionDetails']);
    await TestBed.configureTestingModule({
      declarations: [ ShardedDataflowMigrationDetailsFormComponent ],
      imports: [ HttpClientModule, MatSnackBarModule, ReactiveFormsModule, MatDialogModule, MatInputModule, MatSelectModule, MatIconModule, MatCardModule, MatRadioModule, BrowserAnimationsModule ],
      providers: [
        {
          provide: MatDialogRef,
          useValue: {
            close: () => {},
          },
        },
        {
          provide: MAT_DIALOG_DATA,
          useValue: {
          }
        },
        {
          provide: FetchService,
          useValue: fetchServiceSpy
        },
        {
          provide: DataService,
          useValue: dataServiceSpy
        }
      ],
    })
    .compileComponents();

    fetchServiceSpy.verifyJsonConfiguration.and.returnValue(of(mockMigrationProfile));
    fetchServiceSpy.getConnectionProfiles.and.returnValue(of(mockConnectionProfiles));
    fetchServiceSpy.getStaticIps.and.returnValue(of(mockStaticIps));
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ShardedDataflowMigrationDetailsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('verifyTextJson', () => {
    it('should verify json', () => {
      let formBuilder = new FormBuilder()
      component.migrationProfileForm = formBuilder.group({
        textInput: ['{}'],
      })
      component.verifyTextJson()
      expect(fetchServiceSpy.verifyJsonConfiguration).toHaveBeenCalled()
      expect(component.verifyJson).toEqual(true)
    });

    it('verifyJsonConfiguration API should throw error', () => {
      let formBuilder = new FormBuilder()
      fetchServiceSpy.verifyJsonConfiguration.and.returnValue(throwError(() => new Error("Error"))); 
      component.migrationProfileForm = formBuilder.group({
        textInput: ['{}'],
      })
      component.verifyTextJson()
      expect(fetchServiceSpy.verifyJsonConfiguration).toHaveBeenCalled()
      expect(component.errorVerMsg).toMatch("Error")
    });

    it('parsing json error', () => {
      let formBuilder = new FormBuilder()
      component.migrationProfileForm = formBuilder.group({
        textInput: ['{'],
      })
      component.verifyTextJson()
      expect(component.errorVerMsg).toEqual("Expected property name or '}' in JSON at position 1 (line 1 column 2)")
      expect(component.verifyJson).toEqual(false)
    });
  })
});
