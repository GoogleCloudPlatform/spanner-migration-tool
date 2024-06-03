import { HttpClientModule } from '@angular/common/http';
import { TestBed } from '@angular/core/testing';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import IConv from '../../model/conv'

import { ConversionService } from './conversion.service';
import ISchemaObjectNode from 'src/app/model/schema-object-node';
import { ObjectExplorerNodeType } from 'src/app/app.constants';

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
});
