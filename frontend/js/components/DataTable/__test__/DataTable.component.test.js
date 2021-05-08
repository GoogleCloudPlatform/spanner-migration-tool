import "./../DataTable.component.js";
import "./../../SiteButton/SiteButton.component.js"
let TableData = {
    SpSchema: {
      Name: "actor",
      ColNames: ["actor_id", "first_name", "last_name", "last_update"],
      ColDefs: {
        actor_id: {
          Name: "actor_id",
          T: {
            Name: "INT64",
            Len: 0,
            IsArray: false,
          },
          NotNull: true,
          Comment: "From: actor_id smallint(6)",
        },
        first_name: {
          Name: "first_name",
          T: {
            Name: "STRING",
            Len: 45,
            IsArray: false,
          },
          NotNull: true,
          Comment: "From: first_name varchar(45)",
        },
        last_name: {
          Name: "last_name",
          T: {
            Name: "STRING",
            Len: 45,
            IsArray: false,
          },
          NotNull: true,
          Comment: "From: last_name varchar(45)",
        },
        last_update: {
          Name: "last_update",
          T: {
            Name: "TIMESTAMP",
            Len: 0,
            IsArray: false,
          },
          NotNull: true,
          Comment: "From: last_update timestamp",
        },
      },
      Pks: [
        {
          Col: "actor_id",
          Desc: false,
          seqId: 1,
        },
      ],
      Fks: null,
      Indexes: [
        {
          Name: "idx_actor_last_name",
          Table: "actor",
          Unique: false,
          Keys: [
            {
              Col: "last_name",
              Desc: false,
            },
          ],
        },
      ],
      Parent: "",
      Comment: "Spanner schema for source table actor",
    },
    SrcSchema: {
      Name: "actor",
      ColNames: ["actor_id", "first_name", "last_name", "last_update"],
      ColDefs: {
        actor_id: {
          Name: "actor_id",
          Type: {
            Name: "smallint",
            Mods: [6],
            ArrayBounds: null,
          },
          NotNull: true,
          Unique: true,
          Ignored: {
            Check: false,
            Identity: false,
            Default: false,
            Exclusion: false,
            ForeignKey: false,
            AutoIncrement: true,
          },
        },
        first_name: {
          Name: "first_name",
          Type: {
            Name: "varchar",
            Mods: [45],
            ArrayBounds: null,
          },
          NotNull: true,
          Unique: false,
          Ignored: {
            Check: false,
            Identity: false,
            Default: false,
            Exclusion: false,
            ForeignKey: false,
            AutoIncrement: false,
          },
        },
        last_name: {
          Name: "last_name",
          Type: {
            Name: "varchar",
            Mods: [45],
            ArrayBounds: null,
          },
          NotNull: true,
          Unique: false,
          Ignored: {
            Check: false,
            Identity: false,
            Default: false,
            Exclusion: false,
            ForeignKey: false,
            AutoIncrement: false,
          },
        },
        last_update: {
          Name: "last_update",
          Type: {
            Name: "timestamp",
            Mods: null,
            ArrayBounds: null,
          },
          NotNull: true,
          Unique: false,
          Ignored: {
            Check: false,
            Identity: false,
            Default: true,
            Exclusion: false,
            ForeignKey: false,
            AutoIncrement: false,
          },
        },
      },
      PrimaryKeys: [
        {
          Column: "actor_id",
          Desc: false,
        },
      ],
      ForeignKeys: null,
      Indexes: [
        {
          Name: "idx_actor_last_name",
          Unique: false,
          Keys: [
            {
              Column: "last_name",
              Desc: false,
            },
          ],
        },
      ],
    },
    ToSource: {
      Name: "actor",
      Cols: {
        actor_id: "actor_id",
        first_name: "first_name",
        last_name: "last_name",
        last_update: "last_update",
      },
    },
    ToSpenner: {
      Name: "actor",
      Cols: {
        actor_id: "actor_id",
        first_name: "first_name",
        last_name: "last_name",
        last_update: "last_update",
      },
    },
    currentPageNumber: 0,
    summary: "dummy summary content !!",
  };
test("should data table component render with given data", () => {
  document.body.innerHTML = `<hb-data-table tableName="test table title" tableIndex="0"></hb-data-table>`;
  let dataTable = document.querySelector("hb-data-table");
  expect(dataTable).not.toBe(null);
  expect(dataTable.innerHTML).toBe("");
  expect(dataTable.tableName).toBe('test table title');
  dataTable.data = TableData;
  dataTable.setAttribute("tableName",'sam');
  expect(dataTable.tableName).toBe('sam');
  expect(dataTable.innerHTML).not.toBe("");
  expect(document.querySelector('.fk-card')).toBe(null)
  expect(document.querySelector('.collapse.index-collapse.show')).not.toBe(null)
  expect(document.querySelector('hb-site-button').buttonAction).toBe('createNewSecIndex')
});
