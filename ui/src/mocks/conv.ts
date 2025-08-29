import IConv from "src/app/model/conv";

const mockIConv: IConv = {
    SpSchema: {
        "t1": {
            Name: "table1",
            Id: "t1",
            ColIds: ["c1"],
            ColDefs: {
                "c1": {
                    Id: "c1",
                    Name: "column1",
                    NotNull: false,
                    Comment: "",
                    T: {
                        Name: "STRING",
                        Len: 50,
                        IsArray: false
                    },
                    Opts: {},
                    AutoGen: {
                        Name: "",
                        GenerationType: ""
                    },
                    DefaultValue: {
                        Value: {
                            Statement: "",
                            ExpressionId: ""
                        },
                        IsPresent: false
                    }
                }
            },
            ShardIdColumn: "",
            PrimaryKeys: [],
            ForeignKeys: [],
            CheckConstraints: [],
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
            ParentTable: {Id: "", OnDelete: "", InterleaveType: ""},
            Comment: ""
        }
    },
    SyntheticPKeys: {},
    SrcSchema: {
        "t1": {
            Name: "table1",
            Id: "t1",
            ColIds: ["c1"],
            ColDefs: {
                "c1": {
                    Id: "c1",
                    Name: "column1",
                    NotNull: false,
                    Type: {
                        Name: "STRING",
                        Mods: [],
                        ArrayBounds: []
                    },
                    Ignored: {
                        Check: false,
                        Identity: false,
                        Default: false,
                        Exclusion: false,
                        ForeignKey: false,
                        AutoIncrement: false
                    },
                    AutoGen: {
                        Name: "",
                        GenerationType: ""
                    },
                    DefaultValue: {
                        Value: {
                            Statement: "",
                            ExpressionId: ""
                        },
                        IsPresent: false
                    },
                }
            },
            PrimaryKeys: [],
            ForeignKeys: [],
            CheckConstraints:[],
            Indexes: [
                {
                    Name: "index1",
                    Id: "ind1",
                    Unique: false,
                    Keys: [
                        {
                            ColId: "c1",
                            Desc: false,
                            Order: 1
                        },
                    ]
                },
            ],
            Schema: ""
        }
    },
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
    DatabaseName: "testdb",
    DatabaseType: 'mysql',
    EditorName: 'SampleEditorName',
    SpDialect: 'googlestandardsql',
    IsSharded: false,
    SpSequences: {
        "s1": {
            Id: "s1",
            Name: "Sequence1",
            SequenceKind: "BIT REVERSED POSITIVE"
        },
    },
    SrcSequences: {}
};

export const mockIConv2: IConv = {
    SpSchema: {
        "t1": {
            Name: "table1",
            Id: "t1",
            ColIds: [],
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
            ParentTable: {Id: "", OnDelete: "", InterleaveType: ""},
            CheckConstraints:[],
            Comment: ""
        },
        "t2": {
            Name: "table2",
            Id: "t2",
            ColIds: [],
            ColDefs: {},
            ShardIdColumn: "",
            PrimaryKeys: [],
            ForeignKeys: [],
            Indexes: [],
            ParentTable: {Id: "", OnDelete: "", InterleaveType: ""},
            CheckConstraints:[],
            Comment: ""
        }
    },
    SyntheticPKeys: {},
    SrcSchema: {
        "t1": {
            Name: "table1",
            Id: "t1",
            ColIds: ["c1"],
            ColDefs: {
                "c1": {
                    Id: "c1",
                    Name: "column1",
                    NotNull: false,
                    Type: {
                        Name: "STRING",
                        Mods: [],
                        ArrayBounds: []
                    },
                    Ignored: {
                        Check: false,
                        Identity: false,
                        Default: false,
                        Exclusion: false,
                        ForeignKey: false,
                        AutoIncrement: false
                    },
                    AutoGen: {
                        Name: "",
                        GenerationType: ""
                    },
                    DefaultValue: {
                        Value: {
                            Statement: "",
                            ExpressionId: ""
                        },
                        IsPresent: false
                    }
                }
            },
            CheckConstraints:[],
            PrimaryKeys: [],
            ForeignKeys: [],
            Indexes: [
                {
                    Name: "index1",
                    Id: "ind1",
                    Unique: false,
                    Keys: [
                        {
                            ColId: "c1",
                            Desc: false,
                            Order: 1
                        },
                    ]
                },
            ],
            Schema: ""
        }
    },
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
    DatabaseName: "testdb",
    DatabaseType: 'mysql',
    EditorName: 'SampleEditorName',
    SpDialect: 'googlestandardsql',
    IsSharded: false,
    SpSequences: {},
    SrcSequences: {}
};

export default mockIConv;
