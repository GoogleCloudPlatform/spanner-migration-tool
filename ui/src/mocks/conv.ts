import IConv from "src/app/model/conv";

const mockIConv: IConv = {
    SpSchema: {},
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