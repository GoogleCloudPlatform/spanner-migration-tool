import IConv from "src/app/model/conv";

const mockIConv: IConv = {
    SpSchema: {
        "t1": {
            Name: "table1",
            Id: "t1",
            ColIds:[],
            ColDefs: {},
            ShardIdColumn: "",
            PrimaryKeys: [],
            ForeignKeys: [],
            Indexes: [
                {
                    Name: "index1",
                    Id: "ind1",
                    Unique: false,
                    TableId: "t1",
                    Keys: [
                        {
                            ColId: "c1",
                            Desc: false,
                            Order: 1
                        },
                    ]
                },
            ],
            ParentId: "",
            Comment: ""
        }
    },
    SyntheticPKeys: {},
    SrcSchema: {},
    SchemaIssues: [],
    Rules: [],
    ToSpanner: {},
    ToSource: {},
    UsedNames: {},
    TimezoneOffset: 'UTC',
    Stats: {
        Rows: {},
        GoodRows: {},
        BadRows: {},
        Unexpected: {},
        Reparsed: 0,
    },
    UniquePKey: {},
    SessionName: 'SampleSession',
    DatabaseType: 'SampleDatabaseType',
    DatabaseName: 'SampleDatabaseName',
    EditorName: 'SampleEditorName',
    SpDialect: 'SampleSpDialect',
    IsSharded: false,
};

export const mockIConv2: IConv = {
    SpSchema: {
        "t1": {
            Name: "table1",
            Id: "t1",
            ColIds:[],
            ColDefs: {},
            ShardIdColumn: "",
            PrimaryKeys: [],
            ForeignKeys: [],
            Indexes: [
                {
                    Name: "index1",
                    Id: "ind1",
                    Unique: false,
                    TableId: "t1",
                    Keys: [
                        {
                            ColId: "c1",
                            Desc: false,
                            Order: 1
                        },
                    ]
                },
                {
                    Name: "index2",
                    Id: "ind2",
                    Unique: false,
                    TableId: "t1",
                    Keys: [
                      {
                        ColId: "c2",
                        Desc: false,
                        Order: 1
                      },
                    ]
                  },
            ],
            ParentId: "",
            Comment: ""
        }
    },
    SyntheticPKeys: {},
    SrcSchema: {},
    SchemaIssues: [],
    Rules: [],
    ToSpanner: {},
    ToSource: {},
    UsedNames: {},
    TimezoneOffset: 'UTC',
    Stats: {
        Rows: {},
        GoodRows: {},
        BadRows: {},
        Unexpected: {},
        Reparsed: 0,
    },
    UniquePKey: {},
    SessionName: 'SampleSession',
    DatabaseType: 'SampleDatabaseType',
    DatabaseName: 'SampleDatabaseName',
    EditorName: 'SampleEditorName',
    SpDialect: 'SampleSpDialect',
    IsSharded: false,
};

export default mockIConv;