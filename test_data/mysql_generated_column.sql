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
