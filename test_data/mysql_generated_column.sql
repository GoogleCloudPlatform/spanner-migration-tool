-- MySQL dump for generated column integration tests
--

CREATE TABLE `test_generated_columns` (
  `id` int(11) NOT NULL,
  `col1` int(11) DEFAULT NULL,
  `col2` int(11) DEFAULT NULL,
  `valid_gc` int(11) GENERATED ALWAYS AS ((`col1` + `col2`)) STORED,
  `invalid_gc` varchar(50) GENERATED ALWAYS AS (CAST(STR_TO_DATE('2023-01-01', '%Y-%m-%d') AS CHAR)) STORED,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `test_generated_columns_invalid_pk` (
  `id` int(11) NOT NULL,
  `col1` int(11) DEFAULT NULL,
  `col2` int(11) DEFAULT NULL,
  `invalid_gc_a` int(11) GENERATED ALWAYS AS ((`col1` + `col2`)) STORED,
  `invalid_gc_b` int(11) GENERATED ALWAYS AS ((`col1` + 1)) STORED,
  PRIMARY KEY (`invalid_gc_a`, `invalid_gc_b`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `test_generated_columns_valid_pk` (
 `id` int(11) NOT NULL,
 `col1` int(11) DEFAULT NULL,
 `col2` int(11) DEFAULT NULL,
 `valid_pk_gc` int(11) GENERATED ALWAYS AS ((`col1` + 1)) STORED,
 `valid_pk` int(11) NOT NULL,
 PRIMARY KEY (`valid_pk_gc`, `valid_pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
