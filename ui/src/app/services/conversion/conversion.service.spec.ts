import { HttpClientModule } from '@angular/common/http';
import { TestBed } from '@angular/core/testing';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import IConv, { IInterleavedParent } from '../../model/conv'

import { ConversionService } from './conversion.service';
import ISchemaObjectNode from 'src/app/model/schema-object-node';
import { ObjectExplorerNodeType } from 'src/app/app.constants';
import ICcTabData from 'src/app/model/cc-tab-data';

describe('ConversionService', () => {
  let service: ConversionService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule],
    });
    service = TestBed.inject(ConversionService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('getSpannerSequenceNameFromId', () => {
    let conv: IConv = {} as IConv
    conv.SpSequences ={
      "s1": {
        Name: "Sequence1",
        Id: "s1",
        SequenceKind: "BIT REVERSED POSITIVE"
      },
      "s2": {
        Name: "Sequence2",
        Id: "s2",
        SequenceKind: "BIT REVERSED POSITIVE"
      },
    }
    const id = service.getSpannerSequenceNameFromId("s1", conv)
    expect(id).toEqual("Sequence1");
  });

  it('sortNodeChildren', () => {
    const childrenNodes: ISchemaObjectNode[] = [
      {
        name: "tableA",
        type: ObjectExplorerNodeType.Table,
        pos: -1,
        isSpannerNode: false,
        id: "a",
        parent: "",
        parentId: ""
      },
      {
        name: "tableC",
        type: ObjectExplorerNodeType.Table,
        pos: -1,
        isSpannerNode: false,
        id: "c",
        parent: "",
        parentId: ""
      },
      {
        name: "tableB",
        type: ObjectExplorerNodeType.Table,
        pos: -1,
        isSpannerNode: false,
        id: "b",
        parent: "",
        parentId: ""
      }
    ];
    const node : ISchemaObjectNode = {
      name: `Tables`,
      type: ObjectExplorerNodeType.Tables,
      parent: '',
      pos: -1,
      isSpannerNode: true,
      id: '',
      parentId: '',
      children: childrenNodes
    }
    service.sortNodeChildren(node, 'asc')
    expect(node.children![0].name).toEqual("tableA");
    expect(node.children![2].name).toEqual("tableC");
    service.sortNodeChildren(node, 'desc')
    expect(node.children![0].name).toEqual("tableC");
    expect(node.children![2].name).toEqual("tableA");
  });

  it('getSequenceMapping', () => {
    let conv: IConv = {} as IConv
    conv.SpSequences ={
      "s1": {
        Name: "Sequence1",
        Id: "s1",
        SequenceKind: "BIT REVERSED POSITIVE"
      },
      "s2": {
        Name: "Sequence2",
        Id: "s2",
        SequenceKind: "BIT REVERSED POSITIVE"
      },
    }
    const seq = service.getSequenceMapping("s1", conv)
    expect(seq).toEqual({
      spSeqName: "Sequence1",
      spSequenceKind: "BIT REVERSED POSITIVE",
      spSkipRangeMax: undefined,
      spSkipRangeMin: undefined,
      spStartWithCounter: undefined,
    });
  });

  it('getCheckConstraints when src has more data then spanner', () => {
    let conv: IConv = {} as IConv
    conv.SrcSchema = {
      t1: {
        Name: 'test',
        Id: '1',
        Schema: '',
        ColIds: [],
        ColDefs: {},
        PrimaryKeys: [],
        ForeignKeys: [],

        Indexes: [],
        CheckConstraints: [
          { Id: '1', Name: 'Name1', Expr: 'Expr1' },
          { Id: '2', Name: 'Name2', Expr: 'Expr2' },
        ],
      },
    }
    conv.SpSchema = {
      t1: {
        Name: 'test',
        Id: '1',
        ColIds: [],
        ShardIdColumn: '',
        ColDefs: {},
        PrimaryKeys: [],
        ForeignKeys: [],

        Indexes: [],
        CheckConstraints: [{ Id: '1', Name: 'Name1', Expr: 'Expr1' }],
        ParentTable: {} as IInterleavedParent,
        Comment: '',
      },
    }

    const expected: ICcTabData[] = [
      {
        srcSno: '1',
        srcConstraintName: 'Name1',
        srcCondition: 'Expr1',
        spSno: '1',
        spConstraintName: 'Name1',
        spConstraintCondition: 'Expr1',
        deleteIndex: 'cc1',
      },
      {
        srcSno: '2',
        srcConstraintName: 'Name2',
        srcCondition: 'Expr2',
        spSno: '',
        spConstraintName: '',
        spConstraintCondition: '',
        deleteIndex: 'cc2',
      },
    ]

    const result = service.getCheckConstraints('t1', conv)
    expect(result).toEqual(expected)
  })

  it('getCheckConstraints when spanner is empty', () => {
    let conv: IConv = {} as IConv
    conv.SrcSchema = {
      t1: {
        Name: 'test',
        Id: '1',
        Schema: '',
        ColIds: [],
        ColDefs: {},
        PrimaryKeys: [],
        ForeignKeys: [],

        Indexes: [],
        CheckConstraints: [
          { Id: '1', Name: 'Name1', Expr: 'Expr1' },
          { Id: '2', Name: 'Name2', Expr: 'Expr2' },
        ],
      },
    }
    conv.SpSchema = {
      t1: {
        Name: 'test',
        Id: '1',
        ColIds: [],
        ShardIdColumn: '',
        ColDefs: {},
        PrimaryKeys: [],
        ForeignKeys: [],

        Indexes: [],
        CheckConstraints: [],
        ParentTable: {} as IInterleavedParent,
        Comment: '',
      },
    }

    const expected: ICcTabData[] = [
      {
        srcSno: '1',
        srcConstraintName: 'Name1',
        srcCondition: 'Expr1',
        spSno: '',
        spConstraintName: '',
        spConstraintCondition: '',
        deleteIndex: 'cc1',
      },
      {
        srcSno: '2',
        srcConstraintName: 'Name2',
        srcCondition: 'Expr2',
        spSno: '',
        spConstraintName: '',
        spConstraintCondition: '',
        deleteIndex: 'cc2',
      },
    ];

    const result = service.getCheckConstraints('t1', conv)
    expect(result).toEqual(expected)
  })

  it('getCheckConstraints when src is less than spanner', () => {
    let conv: IConv = {} as IConv
    conv.SrcSchema = {
      t1: {
        Name: 'test',
        Id: '1',
        Schema: '',
        ColIds: [],
        ColDefs: {},
        PrimaryKeys: [],
        ForeignKeys: [],

        Indexes: [],
        CheckConstraints: [{ Id: '1', Name: 'Name1', Expr: 'Expr1' }],
      },
    }
    conv.SpSchema = {
      t1: {
        Name: 'test',
        Id: '1',
        ColIds: [],
        ShardIdColumn: '',
        ColDefs: {},
        PrimaryKeys: [],
        ForeignKeys: [],

        Indexes: [],
        CheckConstraints: [
          { Id: '1', Name: 'Name1', Expr: 'Expr1' },
          { Id: '2', Name: 'Name2', Expr: 'Expr2' },
        ],
        ParentTable: {} as IInterleavedParent,
        Comment: '',
      },
    }

    const expected: ICcTabData[] = [
      {
        srcSno: '1',
        srcConstraintName: 'Name1',
        srcCondition: 'Expr1',
        spSno: '1',
        spConstraintName: 'Name1',
        spConstraintCondition: 'Expr1',
        deleteIndex: 'cc1',
      },
      {
        srcSno: '',
        srcConstraintName: '',
        srcCondition: '',
        spSno: '2',
        spConstraintName: 'Name2',
        spConstraintCondition: 'Expr2',
        deleteIndex: 'cc2',
      },
    ]

    const result = service.getCheckConstraints('t1', conv)
    expect(result).toEqual(expected)
  })
});
