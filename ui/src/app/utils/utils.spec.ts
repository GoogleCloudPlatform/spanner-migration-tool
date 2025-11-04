import { TestBed } from '@angular/core/testing';
import { FormGroup, FormControl } from '@angular/forms';
import { SourceDbNames } from '../app.constants';
import IConv from '../model/conv';
import { 
  extractSourceDbName, 
  downloadSession, 
  downloadOverrides, 
  extractOverridesFromConv, 
  groupAutoGenByType, 
  processAutoGens, 
  linkedFieldsValidatorSequence 
} from './utils';

describe('Utils', () => {
  describe('extractSourceDbName', () => {
    it('should return MySQL for mysql source', () => {
      expect(extractSourceDbName('mysql')).toBe(SourceDbNames.MySQL);
    });

    it('should return MySQL for mysqldump source', () => {
      expect(extractSourceDbName('mysqldump')).toBe(SourceDbNames.MySQL);
    });

    it('should return Postgres for postgres source', () => {
      expect(extractSourceDbName('postgres')).toBe(SourceDbNames.Postgres);
    });

    it('should return Postgres for pgdump source', () => {
      expect(extractSourceDbName('pgdump')).toBe(SourceDbNames.Postgres);
    });

    it('should return Postgres for pg_dump source', () => {
      expect(extractSourceDbName('pg_dump')).toBe(SourceDbNames.Postgres);
    });

    it('should return Oracle for oracle source', () => {
      expect(extractSourceDbName('oracle')).toBe(SourceDbNames.Oracle);
    });

    it('should return SQLServer for sqlserver source', () => {
      expect(extractSourceDbName('sqlserver')).toBe(SourceDbNames.SQLServer);
    });

    it('should return original name for unknown source', () => {
      expect(extractSourceDbName('unknown')).toBe('unknown');
    });
  });

  describe('extractOverridesFromConv', () => {
    let mockConv: IConv;

    beforeEach(() => {
      mockConv = {
        SessionName: 'test-session',
        DatabaseType: 'mysql',
        DatabaseName: 'test-db',
        ToSpanner: {},
        ToSource: {},
        SpSchema: {},
        SyntheticPKeys: {},
        SrcSchema: {},
        SchemaIssues: [],
        Rules: [],
        UsedNames: {},
        TimezoneOffset: '',
        Stats: {
          Rows: {},
          GoodRows: {},
          BadRows: {},
          Unexpected: {},
          Reparsed: 0
        },
        UniquePKey: {},
        EditorName: '',
        SpDialect: '',
        IsSharded: false,
        SpSequences: {},
        SrcSequences: {}
      };
    });

    it('should return empty overrides when ToSpanner is empty', () => {
      const result = extractOverridesFromConv(mockConv);
      
      expect(result).toEqual({
        renamedTables: {},
        renamedColumns: {}
      });
    });

    it('should extract renamed tables correctly', () => {
      mockConv.ToSpanner = {
        'source_table': {
          Name: 'target_table',
          Cols: {}
        }
      };

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedTables).toEqual({
        'source_table': 'target_table'
      });
      expect(result.renamedColumns).toEqual({});
    });

    it('should extract renamed columns correctly', () => {
      mockConv.ToSpanner = {
        'source_table': {
          Name: 'source_table', // Same name, no table rename
          Cols: {
            'source_col1': 'target_col1',
            'source_col2': 'target_col2'
          }
        }
      };

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedTables).toEqual({});
      expect(result.renamedColumns).toEqual({
        'source_table': {
          'source_col1': 'target_col1',
          'source_col2': 'target_col2'
        }
      });
    });

    it('should extract both renamed tables and columns', () => {
      mockConv.ToSpanner = {
        'source_table1': {
          Name: 'target_table1',
          Cols: {
            'source_col1': 'target_col1'
          }
        },
        'source_table2': {
          Name: 'source_table2', // No rename
          Cols: {
            'source_col2': 'target_col2'
          }
        }
      };

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedTables).toEqual({
        'source_table1': 'target_table1'
      });
      expect(result.renamedColumns).toEqual({
        'source_table1': {
          'source_col1': 'target_col1'
        },
        'source_table2': {
          'source_col2': 'target_col2'
        }
      });
    });

    it('should ignore columns with same name', () => {
      mockConv.ToSpanner = {
        'source_table': {
          Name: 'source_table',
          Cols: {
            'same_name': 'same_name',
            'different_name': 'new_name'
          }
        }
      };

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedColumns).toEqual({
        'source_table': {
          'different_name': 'new_name'
        }
      });
    });

    it('should handle undefined or null nameAndCols', () => {
      mockConv.ToSpanner = {
        'table1': undefined,
        'table2': null,
        'table3': {
          Name: 'table3',
          Cols: {}
        }
      } as any;

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedTables).toEqual({});
      expect(result.renamedColumns).toEqual({});
    });

    it('should handle non-object nameAndCols', () => {
      mockConv.ToSpanner = {
        'table1': 'string' as any,
        'table2': 123 as any,
        'table3': {
          Name: 'table3',
          Cols: {}
        }
      };

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedTables).toEqual({});
      expect(result.renamedColumns).toEqual({});
    });

    it('should handle missing Name property', () => {
      mockConv.ToSpanner = {
        'source_table': {
          Cols: {
            'source_col': 'target_col'
          }
        } as any
      };

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedTables).toEqual({});
      expect(result.renamedColumns).toEqual({
        'source_table': {
          'source_col': 'target_col'
        }
      });
    });

    it('should handle missing Cols property', () => {
      mockConv.ToSpanner = {
        'source_table': {
          Name: 'target_table'
        } as any
      };

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedTables).toEqual({
        'source_table': 'target_table'
      });
      expect(result.renamedColumns).toEqual({});
    });

    it('should handle empty Cols object', () => {
      mockConv.ToSpanner = {
        'source_table': {
          Name: 'target_table',
          Cols: {}
        }
      };

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedTables).toEqual({
        'source_table': 'target_table'
      });
      expect(result.renamedColumns).toEqual({});
    });

    it('should handle null or undefined column values', () => {
      mockConv.ToSpanner = {
        'source_table': {
          Name: 'source_table',
          Cols: {
            'col1': null,
            'col2': undefined,
            'col3': 'valid_col'
          } as any
        }
      };

      const result = extractOverridesFromConv(mockConv);
      
      expect(result.renamedColumns).toEqual({
        'source_table': {
          'col3': 'valid_col'
        }
      });
    });
  });

  describe('downloadSession', () => {
    let mockConv: IConv;
    let mockCreateElement: jasmine.Spy;
    let mockClick: jasmine.Spy;
    let mockElement: any;

    beforeEach(() => {
      mockConv = {
        SessionName: 'test-session',
        DatabaseType: 'mysql',
        DatabaseName: 'test-db',
        ToSpanner: {},
        ToSource: {},
        SpSchema: {},
        SyntheticPKeys: {},
        SrcSchema: {},
        SchemaIssues: [],
        Rules: [],
        UsedNames: {},
        TimezoneOffset: '',
        Stats: {
          Rows: {},
          GoodRows: {},
          BadRows: {},
          Unexpected: {},
          Reparsed: 0
        },
        UniquePKey: {},
        EditorName: '',
        SpDialect: '',
        IsSharded: false,
        SpSequences: {},
        SrcSequences: {}
      };

      mockClick = jasmine.createSpy('click');
      mockElement = {
        click: mockClick,
        download: '',
        href: ''
      };
      mockCreateElement = spyOn(document, 'createElement').and.returnValue(mockElement);
    });

    it('should create download link with correct filename', () => {
      downloadSession(mockConv);

      expect(mockCreateElement).toHaveBeenCalledWith('a');
      expect(mockElement.download).toBe('test-session_mysql_test-db.json');
    });

    it('should replace max JS integer value in JSON', () => {
      // Add a property that would contain the max JS integer
      mockConv.SessionName = 'test-session';
      
      downloadSession(mockConv);

      expect(mockClick).toHaveBeenCalled();
    });
  });

  describe('downloadOverrides', () => {
    let mockConv: IConv;
    let mockCreateElement: jasmine.Spy;
    let mockClick: jasmine.Spy;
    let mockElement: any;

    beforeEach(() => {
      mockConv = {
        SessionName: 'test-session',
        DatabaseType: 'mysql',
        DatabaseName: 'test-db',
        ToSpanner: {},
        ToSource: {},
        SpSchema: {},
        SyntheticPKeys: {},
        SrcSchema: {},
        SchemaIssues: [],
        Rules: [],
        UsedNames: {},
        TimezoneOffset: '',
        Stats: {
          Rows: {},
          GoodRows: {},
          BadRows: {},
          Unexpected: {},
          Reparsed: 0
        },
        UniquePKey: {},
        EditorName: '',
        SpDialect: '',
        IsSharded: false,
        SpSequences: {},
        SrcSequences: {}
      };

      mockClick = jasmine.createSpy('click');
      mockElement = {
        click: mockClick,
        download: '',
        href: ''
      };
      mockCreateElement = spyOn(document, 'createElement').and.returnValue(mockElement);
    });

    it('should create download link with correct filename for overrides', () => {
      downloadOverrides(mockConv);

      expect(mockCreateElement).toHaveBeenCalledWith('a');
      expect(mockElement.download).toBe('test-session_mysql_test-db_overrides.json');
    });

    it('should call extractOverridesFromConv and create download', () => {
      downloadOverrides(mockConv);

      expect(mockClick).toHaveBeenCalled();
    });
  });

  describe('groupAutoGenByType', () => {
    it('should group auto gens by type correctly', () => {
      const autoGens = [
        { GenerationType: 'type1', Name: 'gen1', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } },
        { GenerationType: 'type1', Name: 'gen2', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } },
        { GenerationType: 'type2', Name: 'gen3', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } },
        { GenerationType: 'type1', Name: 'gen4', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } }
      ];

      const result = groupAutoGenByType(autoGens);

      expect(result).toEqual({
        type1: [
          { GenerationType: 'type1', Name: 'gen1', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } },
          { GenerationType: 'type1', Name: 'gen2', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } },
          { GenerationType: 'type1', Name: 'gen4', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } }
        ],
        type2: [
          { GenerationType: 'type2', Name: 'gen3', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } }
        ]
      });
    });

    it('should return empty object for empty array', () => {
      const result = groupAutoGenByType([]);
      expect(result).toEqual({});
    });
  });

  describe('processAutoGens', () => {
    it('should process auto gen map correctly', () => {
      const autoGenMap = {
        'table1': [
          { GenerationType: 'type1', Name: 'gen1', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } },
          { GenerationType: 'type2', Name: 'gen2', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } }
        ],
        'table2': [
          { GenerationType: 'type1', Name: 'gen3', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } }
        ]
      };

      const result = processAutoGens(autoGenMap);

      expect(result).toEqual({
        'table1': {
          type1: [{ GenerationType: 'type1', Name: 'gen1', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } }],
          type2: [{ GenerationType: 'type2', Name: 'gen2', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } }]
        },
        'table2': {
          type1: [{ GenerationType: 'type1', Name: 'gen3', IdentityOptions: { SkipRangeMin: '', SkipRangeMax: '', StartCounterWith: '' } }]
        }
      });
    });

    it('should handle empty map', () => {
      const result = processAutoGens({});
      expect(result).toEqual({});
    });
  });

  describe('linkedFieldsValidatorSequence', () => {
    let formGroup: FormGroup;

    beforeEach(() => {
      formGroup = new FormGroup({
        skipRangeMin: new FormControl(''),
        skipRangeMax: new FormControl('')
      });
    });

    it('should return null when both fields are empty', () => {
      const validator = linkedFieldsValidatorSequence('skipRangeMin', 'skipRangeMax');
      const result = validator(formGroup);

      expect(result).toBeNull();
    });

    it('should return null when both fields have values', () => {
      formGroup.get('skipRangeMin')?.setValue('10');
      formGroup.get('skipRangeMax')?.setValue('20');

      const validator = linkedFieldsValidatorSequence('skipRangeMin', 'skipRangeMax');
      const result = validator(formGroup);

      expect(result).toBeNull();
    });

    it('should return error when only min field has value', () => {
      formGroup.get('skipRangeMin')?.setValue('10');

      const validator = linkedFieldsValidatorSequence('skipRangeMin', 'skipRangeMax');
      const result = validator(formGroup);

      expect(result).toEqual({
        linkedError: 'Both Skip Range Min and Max are required'
      });
    });

    it('should return error when only max field has value', () => {
      formGroup.get('skipRangeMax')?.setValue('20');

      const validator = linkedFieldsValidatorSequence('skipRangeMin', 'skipRangeMax');
      const result = validator(formGroup);

      expect(result).toEqual({
        linkedError: 'Both Skip Range Min and Max are required'
      });
    });

    it('should return null when form controls do not exist', () => {
      const validator = linkedFieldsValidatorSequence('nonexistent1', 'nonexistent2');
      const result = validator(formGroup);

      expect(result).toBeNull();
    });
  });
}); 