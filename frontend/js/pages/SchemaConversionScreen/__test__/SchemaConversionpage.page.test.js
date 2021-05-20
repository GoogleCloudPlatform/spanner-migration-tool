import "./../SchemaConversionScreen.page.js";
import "../../../components/SiteButton/SiteButton.component.js";
import "./../../../components/LoadingSpinner/LoadingSpinner.component.js";

let TableData = {
  currentTab: "reportTab",
  tableBorderData: { actor: "yellow",address:"blue" },
  searchInputValue: {
    ddlTab: "",
    reportTab: "",
    summaryTab: "",
  },
  tableData: {
    reportTabContent: {
      SpSchema: {
        actor: {
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
        address: {
            Name: "address",
        }

      },
    },
    ddlTabContent: {},
    summaryTabContent: {
      actor: "Warnings/n 1) Column 'actor_id' is an autoincrement column. Spanner does not support auto_increment attribute./n2) Some columns have default values which Spanner does not support e.g. column 'last_update'./nNote/n1) Some columns will consume more storage in Spanner e.g. for column 'actor_id', source DB type smallint(6) is mapped to Spanner type int64. ",
      address:"Warnings/n1) Column 'address_id' is an autoincrement column. Spanner does not support auto_increment attribute./n2) Some columns have default values which Spanner does not support e.g. column 'last_update'./nNote/n1) Some columns will consume more storage in Spanner e.g. for column 'address_id', source DB type smallint(6) is mapped to Spanner type int64./n"
    },
  },
};

afterEach(() => {
  while (document.body.firstChild) {
    document.body.removeChild(document.body.firstChild);
  }
});

describe('empty data test',()=>{

  afterEach(() => {
    while (document.body.firstChild) {
      document.body.removeChild(document.body.firstChild);
    }
  });
  
  test("empty data test", () => {
    document.body.innerHTML =
      "<div><hb-loading-spinner></hb-loading-spinner><hb-schema-conversion-screen></hb-schema-conversion-screen></div>";
    let btn = document.getElementsByTagName("hb-site-button");
    expect(btn.length).toBe(0);
  });
})

describe("rendering test", () => {
  beforeEach(() => {
    document.body.innerHTML =
      '<div><hb-loading-spinner></hb-loading-spinner><hb-schema-conversion-screen testing = "true"></hb-schema-conversion-screen></div>';
    let page = document.querySelector("hb-schema-conversion-screen");
    page.Data = TableData;
  });

  afterEach(() => {
    while (document.body.firstChild) {
      document.body.removeChild(document.body.firstChild);
    }
  });

  test("site button rendering test", () => {
    let btn = document.getElementsByTagName("hb-site-button");
    expect(btn.length).toBe(3);
  });

  test("tabs rendering test", () => {
    let tabs = document.querySelector("hb-tab");
    expect(tabs).not.toBe(null);
  });

  test("search bar rendering test", () => {
    let searchbar = document.querySelector("hb-search");
    expect(searchbar).not.toBe(null);
  });

  test("carausel rendering test", () => {
    let totalcarausel = document.querySelectorAll("hb-table-carousel");
    expect(totalcarausel.length).toBe(2);
  });

});
