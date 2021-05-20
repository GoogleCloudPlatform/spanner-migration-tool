import "./../DataTable.component.js";
import "./../../SiteButton/SiteButton.component.js"

let TableData = {
      "SpSchema": {
          "Name": "payment",
          "ColNames": [
              "payment_id",
              "customer_id",
              "staff_id",
              "rental_id",
              "amount",
              "payment_date",
              "last_update"
          ],
          "ColDefs": {
              "amount": {
                  "Name": "amount",
                  "T": {
                      "Name": "NUMERIC",
                      "Len": 0,
                      "IsArray": false
                  },
                  "NotNull": true,
                  "Comment": "From: amount decimal(5,2)"
              },
              "customer_id": {
                  "Name": "customer_id",
                  "T": {
                      "Name": "INT64",
                      "Len": 0,
                      "IsArray": false
                  },
                  "NotNull": true,
                  "Comment": "From: customer_id smallint(6)"
              },
              "last_update": {
                  "Name": "last_update",
                  "T": {
                      "Name": "TIMESTAMP",
                      "Len": 0,
                      "IsArray": false
                  },
                  "NotNull": false,
                  "Comment": "From: last_update timestamp"
              },
              "payment_date": {
                  "Name": "payment_date",
                  "T": {
                      "Name": "TIMESTAMP",
                      "Len": 0,
                      "IsArray": false
                  },
                  "NotNull": true,
                  "Comment": "From: payment_date datetime"
              },
              "payment_id": {
                  "Name": "payment_id",
                  "T": {
                      "Name": "INT64",
                      "Len": 0,
                      "IsArray": false
                  },
                  "NotNull": true,
                  "Comment": "From: payment_id smallint(6)"
              },
              "rental_id": {
                  "Name": "rental_id",
                  "T": {
                      "Name": "INT64",
                      "Len": 0,
                      "IsArray": false
                  },
                  "NotNull": false,
                  "Comment": "From: rental_id int(11)"
              },
              "staff_id": {
                  "Name": "staff_id",
                  "T": {
                      "Name": "INT64",
                      "Len": 0,
                      "IsArray": false
                  },
                  "NotNull": true,
                  "Comment": "From: staff_id tinyint(4)"
              }
          },
          "Pks": [
              {
                  "Col": "payment_id",
                  "Desc": false,
                  "seqId": 1
              }
          ],
          "Fks": [
              {
                  "Name": "fk_payment_rental",
                  "Columns": [
                      "rental_id"
                  ],
                  "ReferTable": "rental",
                  "ReferColumns": [
                      "rental_id"
                  ]
              },
              {
                  "Name": "fk_payment_customer",
                  "Columns": [
                      "customer_id"
                  ],
                  "ReferTable": "customer",
                  "ReferColumns": [
                      "customer_id"
                  ]
              },
              {
                  "Name": "fk_payment_staff",
                  "Columns": [
                      "staff_id"
                  ],
                  "ReferTable": "staff",
                  "ReferColumns": [
                      "staff_id"
                  ]
              }
          ],
          "Indexes": [
              {
                  "Name": "idx_fk_staff_id_59",
                  "Table": "payment",
                  "Unique": false,
                  "Keys": [
                      {
                          "Col": "staff_id",
                          "Desc": false
                      }
                  ]
              },
              {
                  "Name": "idx_fk_customer_id_60",
                  "Table": "payment",
                  "Unique": false,
                  "Keys": [
                      {
                          "Col": "customer_id",
                          "Desc": false
                      }
                  ]
              }
          ],
          "Parent": "",
          "Comment": "Spanner schema for source table payment"
      },
      "SrcSchema": {
          "Name": "payment",
          "ColNames": [
              "payment_id",
              "customer_id",
              "staff_id",
              "rental_id",
              "amount",
              "payment_date",
              "last_update"
          ],
          "ColDefs": {
              "amount": {
                  "Name": "amount",
                  "Type": {
                      "Name": "decimal",
                      "Mods": [
                          5,
                          2
                      ],
                      "ArrayBounds": null
                  },
                  "NotNull": true,
                  "Unique": false,
                  "Ignored": {
                      "Check": false,
                      "Identity": false,
                      "Default": false,
                      "Exclusion": false,
                      "ForeignKey": false,
                      "AutoIncrement": false
                  }
              },
              "customer_id": {
                  "Name": "customer_id",
                  "Type": {
                      "Name": "smallint",
                      "Mods": [
                          6
                      ],
                      "ArrayBounds": null
                  },
                  "NotNull": true,
                  "Unique": false,
                  "Ignored": {
                      "Check": false,
                      "Identity": false,
                      "Default": false,
                      "Exclusion": false,
                      "ForeignKey": false,
                      "AutoIncrement": false
                  }
              },
              "last_update": {
                  "Name": "last_update",
                  "Type": {
                      "Name": "timestamp",
                      "Mods": null,
                      "ArrayBounds": null
                  },
                  "NotNull": false,
                  "Unique": false,
                  "Ignored": {
                      "Check": false,
                      "Identity": false,
                      "Default": true,
                      "Exclusion": false,
                      "ForeignKey": false,
                      "AutoIncrement": false
                  }
              },
              "payment_date": {
                  "Name": "payment_date",
                  "Type": {
                      "Name": "datetime",
                      "Mods": null,
                      "ArrayBounds": null
                  },
                  "NotNull": true,
                  "Unique": false,
                  "Ignored": {
                      "Check": false,
                      "Identity": false,
                      "Default": false,
                      "Exclusion": false,
                      "ForeignKey": false,
                      "AutoIncrement": false
                  }
              },
              "payment_id": {
                  "Name": "payment_id",
                  "Type": {
                      "Name": "smallint",
                      "Mods": [
                          6
                      ],
                      "ArrayBounds": null
                  },
                  "NotNull": true,
                  "Unique": true,
                  "Ignored": {
                      "Check": false,
                      "Identity": false,
                      "Default": false,
                      "Exclusion": false,
                      "ForeignKey": false,
                      "AutoIncrement": true
                  }
              },
              "rental_id": {
                  "Name": "rental_id",
                  "Type": {
                      "Name": "int",
                      "Mods": [
                          11
                      ],
                      "ArrayBounds": null
                  },
                  "NotNull": false,
                  "Unique": false,
                  "Ignored": {
                      "Check": false,
                      "Identity": false,
                      "Default": false,
                      "Exclusion": false,
                      "ForeignKey": false,
                      "AutoIncrement": false
                  }
              },
              "staff_id": {
                  "Name": "staff_id",
                  "Type": {
                      "Name": "tinyint",
                      "Mods": [
                          4
                      ],
                      "ArrayBounds": null
                  },
                  "NotNull": true,
                  "Unique": false,
                  "Ignored": {
                      "Check": false,
                      "Identity": false,
                      "Default": false,
                      "Exclusion": false,
                      "ForeignKey": false,
                      "AutoIncrement": false
                  }
              }
          },
          "PrimaryKeys": [
              {
                  "Column": "payment_id",
                  "Desc": false
              }
          ],
          "ForeignKeys": [
              {
                  "Name": "fk_payment_rental",
                  "Columns": [
                      "rental_id"
                  ],
                  "ReferTable": "rental",
                  "ReferColumns": [
                      "rental_id"
                  ],
                  "OnDelete": "SET NULL",
                  "OnUpdate": "CASCADE"
              },
              {
                  "Name": "fk_payment_customer",
                  "Columns": [
                      "customer_id"
                  ],
                  "ReferTable": "customer",
                  "ReferColumns": [
                      "customer_id"
                  ],
                  "OnDelete": "RESTRICT",
                  "OnUpdate": "CASCADE"
              },
              {
                  "Name": "fk_payment_staff",
                  "Columns": [
                      "staff_id"
                  ],
                  "ReferTable": "staff",
                  "ReferColumns": [
                      "staff_id"
                  ],
                  "OnDelete": "RESTRICT",
                  "OnUpdate": "CASCADE"
              }
          ],
          "Indexes": [
              {
                  "Name": "idx_fk_staff_id",
                  "Unique": false,
                  "Keys": [
                      {
                          "Column": "staff_id",
                          "Desc": false
                      }
                  ]
              },
              {
                  "Name": "idx_fk_customer_id",
                  "Unique": false,
                  "Keys": [
                      {
                          "Column": "customer_id",
                          "Desc": false
                      }
                  ]
              }
          ]
      },
      "ToSource": {
          "Name": "payment",
          "Cols": {
              "amount": "amount",
              "customer_id": "customer_id",
              "last_update": "last_update",
              "payment_date": "payment_date",
              "payment_id": "payment_id",
              "rental_id": "rental_id",
              "staff_id": "staff_id"
          }
      },
      "ToSpanner": {
          "Name": "payment",
          "Cols": {
              "amount": "amount",
              "customer_id": "customer_id",
              "last_update": "last_update",
              "payment_date": "payment_date",
              "payment_id": "payment_id",
              "rental_id": "rental_id",
              "staff_id": "staff_id"
          }
      },
      "currentPageNumber": 0,
      "summary":"dummy summary content"
  }

  describe('dataTable tests',()=>{

        beforeEach(()=>{
            document.body.innerHTML = `<hb-data-table tableName="test table title" tableIndex="0"></hb-data-table>`;
        })

        test('should not render if data not passed ', () => {
            let dataTable = document.querySelector("hb-data-table");
        expect(dataTable).not.toBe(null);
        expect(dataTable.innerHTML).toBe("");
        })
        

        test("data table component should render with given data", () => {
        let dataTable = document.querySelector("hb-data-table");
        expect(dataTable).not.toBe(null);
        expect(dataTable.innerHTML).toBe("");
        expect(dataTable.tableName).toBe('test table title');
        dataTable.data = TableData;
        dataTable.setAttribute("tableName",'sam');
        expect(dataTable.tableName).toBe('sam');
        expect(dataTable.innerHTML).not.toBe("");
        expect(document.querySelector('.fk-card')).not.toBe(null)
        expect(document.querySelector('.collapse.index-collapse.show')).not.toBe(null)
        expect(document.querySelector('hb-site-button').buttonAction).toBe('createNewSecIndex')
        });

        test('pagination section should render',()=>{
        let dataTable = document.querySelector("hb-data-table");
        dataTable.data = TableData;
        expect(document.querySelector('#pre-btn0')).not.toBe(null)
        expect(document.querySelector('.pagination-number')).not.toBe(null)
        expect(document.querySelector('#next-btn0')).not.toBe(null)

        })

        test('foreign key table should render ', ()=>{
        let dataTable = document.querySelector("hb-data-table");
        dataTable.data = TableData;
        let fkSection = document.querySelectorAll('#fk-table-body-0 > tr')
        expect(fkSection.length).toBe(TableData.SrcSchema.ForeignKeys.length)
        
        })

        test('secondary index table should render',()=>{
        let dataTable = document.querySelector("hb-data-table");
        dataTable.data = TableData;
        let fkSection = document.querySelectorAll('.index-acc-table.fk-table > tbody > tr')
        expect(fkSection.length).toBe(TableData.SrcSchema.Indexes.length)
        expect(document.querySelector(".new-index-button")).not.toBe(null)
        expect(document.querySelector(".new-index-button")).not.toBe(null)
        })

        test('summary component should render', ()=>{
        let dataTable = document.querySelector("hb-data-table");
        dataTable.data = TableData;
        expect(document.querySelector('hb-list-table')).not.toBe(null)
        expect(document.querySelector('hb-list-table')).not.toBe('undefined')
        expect(document.querySelector('hb-list-table').getAttribute('dta')).toBe("dummy summary content")
        })

})




