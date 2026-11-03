import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { Dialect, SourceDbNames } from 'src/app/app.constants';
import { DataService } from 'src/app/services/data/data.service';
import { FetchService } from 'src/app/services/fetch/fetch.service';
import { of } from 'rxjs';

import { AddNewColumnComponent } from './add-new-column.component';

describe('AddNewColumnComponent', () => {
  let component: AddNewColumnComponent;
  let fixture: ComponentFixture<AddNewColumnComponent>;
  let mockDataService: { addColumn: ReturnType<typeof vi.fn> };
  let mockFetchService: { getAutoGenMap: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    mockDataService = {
      addColumn: vi.fn(),
    };
    mockFetchService = {
      getAutoGenMap: vi.fn().mockReturnValue(of({})),
    };

    await TestBed.configureTestingModule({
      declarations: [AddNewColumnComponent],
      imports: [ReactiveFormsModule, MatSnackBarModule, MatDialogModule, MatSelectModule, MatInputModule, BrowserAnimationsModule],
      providers: [
        {
          provide: MatDialogRef,
          useValue: {
            close: () => { },
          },
        },
        {
          provide: MAT_DIALOG_DATA,
          useValue: {
            dialect: Dialect.GoogleStandardSQLDialect,
            tableId: 't1',
          }
        },
        {
          provide: DataService,
          useValue: mockDataService
        },
        {
          provide: FetchService,
          useValue: mockFetchService
        },
        provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting()
      ]
    })
      .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AddNewColumnComponent);
    component = fixture.componentInstance;
    component.dialect = Dialect.GoogleStandardSQLDialect;
    fixture.detectChanges();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should filter out JSON datatype for Cassandra source', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockReturnValue(SourceDbNames.Cassandra);
    fixture = TestBed.createComponent(AddNewColumnComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    expect(component.datatypes).not.toContain('JSON');
    expect(component.datatypes).toContain('STRING');
  });
});
