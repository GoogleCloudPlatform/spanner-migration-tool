-- PostgreSQL dump for generated column and default value integration tests.
--

CREATE TABLE test_generated_columns (
    id integer NOT NULL,
    col1 integer,
    col2 integer,
    name character varying(50),
    valid_gc integer GENERATED ALWAYS AS ((col1 + col2)) STORED,
    invalid_gc character varying(50) GENERATED ALWAYS AS (((id)::text || (name)::text)) STORED,
    PRIMARY KEY (id)
);

CREATE TABLE test_generated_columns_invalid_pk (
    id integer NOT NULL,
    col1 integer,
    col2 integer,
    invalid_gc_a integer GENERATED ALWAYS AS ((col1 + col2)) STORED,
    invalid_gc_b integer GENERATED ALWAYS AS ((col1 + 1)) STORED,
    PRIMARY KEY (invalid_gc_a, invalid_gc_b)
);

CREATE TABLE test_generated_columns_valid_pk (
    id integer NOT NULL,
    col1 integer,
    col2 integer,
    valid_pk_gc integer GENERATED ALWAYS AS ((col1 + 1)) STORED,
    valid_pk integer NOT NULL,
    PRIMARY KEY (valid_pk_gc, valid_pk)
);

CREATE TABLE test_default_values (
    id bigint NOT NULL,
    d_int integer DEFAULT 42,
    d_bigint bigint DEFAULT '9000000000'::bigint,
    d_neg integer DEFAULT '-7'::integer,
    d_str character varying(20) DEFAULT 'NEW'::character varying,
    d_bool boolean DEFAULT true,
    d_numeric numeric(10,2) DEFAULT 3.14,
    d_date date DEFAULT '2020-01-01'::date,
    d_null integer DEFAULT NULL,
    PRIMARY KEY (id)
);

-- pg_dump never includes generated columns; Spanner computes them.
COPY test_generated_columns (id, col1, col2, name) FROM stdin;
1	10	20	abc
2	5	7	xyz
\.

COPY test_generated_columns_valid_pk (id, col1, col2, valid_pk) FROM stdin;
1	10	20	100
\.

-- Only id is given, so the other columns get their defaults.
COPY test_default_values (id) FROM stdin;
1
\.
