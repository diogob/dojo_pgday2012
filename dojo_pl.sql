--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: test; Type: SCHEMA; Schema: -; Owner: diogo
--

CREATE SCHEMA test;


ALTER SCHEMA test OWNER TO diogo;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = test, pg_catalog;

--
-- Name: assert(boolean, text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert(assertion boolean, msg text) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Raises an exception (msg) if assertion is false.
-- 
-- assertion may not be NULL.
BEGIN
  IF assertion IS NULL THEN
    RAISE EXCEPTION 'Assertion test may not be NULL.';
  END IF;
  
  IF NOT assertion THEN
    RAISE EXCEPTION '%', msg;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert(assertion boolean, msg text) OWNER TO diogo;

--
-- Name: assert_column(text, anyarray); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_column(call text, expected anyarray) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Implicit column version of assert_column
BEGIN
  PERFORM test.assert_column(call, expected, NULL);
END;
$$;


ALTER FUNCTION test.assert_column(call text, expected anyarray) OWNER TO diogo;

--
-- Name: assert_column(text, anyarray, text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_column(call text, expected anyarray, colname text) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Raises an exception if SELECT colname FROM call != expected (in order).
--
-- If colname is NULL or omitted, the first column of call's output will be used.
--
-- 'call' can be any table, view, or procedure that returns records.
-- 'expected' MUST be an array of the same type as colname. If necessary, cast it using :: to avoid
-- the error 'type of "record1._assert_column_result" does not match that when preparing the plan'.
-- TODO: can this function detect the type and cast it for the user?
-- 
-- Example:
--    PERFORM test.assert_column(
--      'get_favorite_user_ids(' || user_id || ');',
--      ARRAY[24, 10074, 87321], 'user_id');
-- 
DECLARE
  record1       record;
  record2       record;
  curs_base     refcursor;
  curs_expected refcursor;
  firstname     text;
  found_1       boolean;
  lower_bound   int;
  base_type     text;
  msg           text;
BEGIN
  -- Dump the call output into a temp table
  IF colname IS NULL THEN
    -- No colname; instead, create the temp table and then read the catalog
    -- to grab the name of the first column.
    EXECUTE 'CREATE TEMPORARY TABLE _test_assert_column_base AS ' || test.statement(call);
    SELECT INTO firstname a.attname
      FROM pg_class c LEFT JOIN pg_attribute a ON c.oid = a.attrelid
      WHERE c.relname = '_test_assert_column_base'
      -- "The number of the column. Ordinary columns are numbered from 1 up.
      -- System columns, such as oid, have (arbitrary) negative numbers"
      AND a.attnum >= 1
      ORDER BY a.attnum;
    EXECUTE 'ALTER TABLE _test_assert_column_base RENAME ' || firstname || ' TO _assert_column_result;';
  ELSE
    EXECUTE 'CREATE TEMPORARY TABLE _test_assert_column_base AS ' ||
      'SELECT ' || colname || ' AS _assert_column_result FROM ' || call || ';';
  END IF;
  SELECT INTO base_type pgt.typname
    FROM pg_attribute pga LEFT JOIN pg_type pgt ON pga.atttypid = pgt.oid
    WHERE pga.attrelid = (SELECT oid FROM pg_class WHERE relname = '_test_assert_column_base')
    AND pga.attnum = 1;
  -- Casting to ::name doesn't work so well.
  IF base_type = 'name' THEN
    base_type := 'text';
  END IF;
  
  -- Dump the provided array into a temp table
  -- Use EXECUTE for all statements involving this table so its query plan
  -- doesn't get cached and re-used (or subsequent calls will fail).
  EXECUTE 'CREATE TEMPORARY TABLE _test_assert_column_expected (LIKE _test_assert_column_base);';
  lower_bound = array_lower(expected, 1);
  IF lower_bound iS NOT NULL THEN
    FOR i IN lower_bound..array_upper(expected, 1)
    LOOP
      IF expected[i] IS NULL THEN
        EXECUTE 'INSERT INTO _test_assert_column_expected (_assert_column_result) VALUES (NULL);';
      ELSEIF base_type IN ('text', 'varchar', 'char', 'bytea', 'date', 'timestamp', 'timestamptz', 'time', 'timetz') THEN
        EXECUTE 'INSERT INTO _test_assert_column_expected (_assert_column_result) VALUES ('
                || quote_literal(expected[i]) || ');';
      ELSE
        EXECUTE 'INSERT INTO _test_assert_column_expected (_assert_column_result) VALUES ('
                || expected[i] || ');';
      END IF;
    END LOOP;
  END IF;
  
  -- Compare the two tables in order.
  <<TRY>>
  BEGIN
    OPEN curs_base FOR EXECUTE 'SELECT * FROM _test_assert_column_base';
    OPEN curs_expected FOR EXECUTE 'SELECT * FROM _test_assert_column_expected';
    LOOP
      FETCH curs_base INTO record1;
      found_1 := FOUND;
      FETCH curs_expected INTO record2;
      IF FOUND THEN
        IF NOT found_1 THEN
          PERFORM test.fail('element: ' || record2._assert_column_result || ' not found in call: ' || call);
        END IF;
      ELSE
        IF NOT found_1 THEN
          EXIT;
        ELSE
          PERFORM test.fail('record: ' || record1._assert_column_result || ' not found in array: ' || array_to_string(expected, ', '));
        END IF;
      END IF;
      PERFORM test.assert_equal(record1._assert_column_result, record2._assert_column_result);
    END LOOP;
  EXCEPTION WHEN OTHERS THEN
    DROP TABLE _test_assert_column_base;
    EXECUTE 'DROP TABLE _test_assert_column_expected';
    msg := SQLERRM;
    RAISE EXCEPTION '%', msg;
  END;
  
  CLOSE curs_base;
  CLOSE curs_expected;
  DROP TABLE _test_assert_column_base;
  EXECUTE 'DROP TABLE _test_assert_column_expected';
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_column(call text, expected anyarray, colname text) OWNER TO diogo;

--
-- Name: assert_empty(text[]); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_empty(calls text[]) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Raises an exception if the given calls have any rows.
DECLARE
  result      bool;
  failed      text[] DEFAULT '{}'::text[];
  failed_len  int;
BEGIN
  IF array_lower(calls, 1) IS NOT NULL THEN
    FOR i in array_lower(calls, 1)..array_upper(calls, 1)
    LOOP
      EXECUTE 'SELECT EXISTS (' || test.statement(calls[i]) || ');' INTO result;
      IF result THEN
        failed := failed || ('"' || btrim(calls[i]) || '"');
      END IF;
    END LOOP;
  END IF;
  
  IF array_lower(failed, 1) IS NOT NULL THEN
    failed_len := (array_upper(failed, 1) - array_lower(failed, 1)) + 1;
    IF failed_len = 1 THEN
      PERFORM test.fail('The call ' || array_to_string(failed, ', ') || ' is not empty.');
    ELSEIF failed_len > 1 THEN
      PERFORM test.fail('The calls ' || array_to_string(failed, ', ') || ' are not empty.');
    END IF;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_empty(calls text[]) OWNER TO diogo;

--
-- Name: assert_empty(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_empty(call text) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Raises an exception if the given call returns any rows.
DECLARE
  result    bool;
BEGIN
  EXECUTE 'SELECT EXISTS (' || test.statement(call) || ');' INTO result;
  IF result THEN
    PERFORM test.fail('The call "' || call || '" is not empty.');
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_empty(call text) OWNER TO diogo;

--
-- Name: assert_equal(anyelement, anyelement); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_equal(elem_1 anyelement, elem_2 anyelement) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Raises an exception if elem_1 is not equal to elem_2.
-- 
-- The two arguments must be of the same type. If they are not,
-- you will receive "ERROR:  invalid input syntax ..."
BEGIN
  IF ((elem_1 IS NULL AND NOT (elem_2 IS NULL)) OR
      (elem_2 IS NULL AND NOT (elem_1 IS NULL)) OR
      elem_1 != elem_2) THEN
    RAISE EXCEPTION '% != %', elem_1, elem_2;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_equal(elem_1 anyelement, elem_2 anyelement) OWNER TO diogo;

--
-- Name: assert_greater_than(anyelement, anyelement); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_greater_than(elem_1 anyelement, elem_2 anyelement) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Raises an exception if elem_1 <= elem_2
-- 
-- The two arguments must be of the same type. If they are not,
-- you will receive "ERROR:  invalid input syntax ..."
BEGIN
  IF (elem_1 IS NULL or elem_2 IS NULL) THEN
    RAISE EXCEPTION 'Assertion arguments may not be NULL.';
  END IF;
  IF NOT (elem_1 > elem_2) THEN
    RAISE EXCEPTION '% not > %', elem_1, elem_2;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_greater_than(elem_1 anyelement, elem_2 anyelement) OWNER TO diogo;

--
-- Name: assert_greater_than_or_equal(anyelement, anyelement); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_greater_than_or_equal(elem_1 anyelement, elem_2 anyelement) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Raises an exception if elem_1 < elem_2
-- 
-- The two arguments must be of the same type. If they are not,
-- you will receive "ERROR:  invalid input syntax ..."
BEGIN
  IF (elem_1 IS NULL or elem_2 IS NULL) THEN
    RAISE EXCEPTION 'Assertion arguments may not be NULL.';
  END IF;
  IF NOT (elem_1 >= elem_2) THEN
    RAISE EXCEPTION '% not >= %', elem_1, elem_2;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_greater_than_or_equal(elem_1 anyelement, elem_2 anyelement) OWNER TO diogo;

--
-- Name: assert_less_than(anyelement, anyelement); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_less_than(elem_1 anyelement, elem_2 anyelement) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Raises an exception if elem_1 >= elem_2
-- 
-- The two arguments must be of the same type. If they are not,
-- you will receive "ERROR:  invalid input syntax ..."
BEGIN
  IF (elem_1 IS NULL or elem_2 IS NULL) THEN
    RAISE EXCEPTION 'Assertion arguments may not be NULL.';
  END IF;
  IF NOT (elem_1 < elem_2) THEN
    RAISE EXCEPTION '% not < %', elem_1, elem_2;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_less_than(elem_1 anyelement, elem_2 anyelement) OWNER TO diogo;

--
-- Name: assert_less_than_or_equal(anyelement, anyelement); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_less_than_or_equal(elem_1 anyelement, elem_2 anyelement) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Raises an exception if elem_1 > elem_2
-- 
-- The two arguments must be of the same type. If they are not,
-- you will receive "ERROR:  invalid input syntax ..."
BEGIN
  IF (elem_1 IS NULL or elem_2 IS NULL) THEN
    RAISE EXCEPTION 'Assertion arguments may not be NULL.';
  END IF;
  IF NOT (elem_1 <= elem_2) THEN
    RAISE EXCEPTION '% not <= %', elem_1, elem_2;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_less_than_or_equal(elem_1 anyelement, elem_2 anyelement) OWNER TO diogo;

--
-- Name: assert_not_empty(text[]); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_not_empty(calls text[]) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Raises an exception if the given calls have no rows.
DECLARE
  result      bool;
  failed      text[];
  failed_len  int;
BEGIN
  IF array_lower(calls, 1) IS NOT NULL THEN
    FOR i in array_lower(calls, 1)..array_upper(calls, 1)
    LOOP
      EXECUTE 'SELECT EXISTS (' || test.statement(calls[i]) || ');' INTO result;
      IF NOT result THEN
        failed := failed || ('"' || btrim(calls[i]) || '"');
      END IF;
    END LOOP;
  END IF;
  
  IF array_lower(failed, 1) IS NULL THEN
    -- failed is an empty array (no failures).
    NULL;
  ELSE
    failed_len := (array_upper(failed, 1) - array_lower(failed, 1)) + 1;
    IF failed_len = 1 THEN
      PERFORM test.fail('The call ' || array_to_string(failed, ', ') || ' is empty.');
    ELSEIF failed_len > 1 THEN
      PERFORM test.fail('The calls ' || array_to_string(failed, ', ') || ' are empty.');
    END IF;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_not_empty(calls text[]) OWNER TO diogo;

--
-- Name: assert_not_empty(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_not_empty(call text) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Raises an exception if the given table has no rows.
DECLARE
  result    bool;
BEGIN
  EXECUTE 'SELECT EXISTS (' || test.statement(call) || ');' INTO result;
  IF NOT result THEN
    PERFORM test.fail('The call "' || call || '" is empty.');
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_not_empty(call text) OWNER TO diogo;

--
-- Name: assert_not_equal(anyelement, anyelement); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_not_equal(elem_1 anyelement, elem_2 anyelement) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Raises an exception if elem_1 is equal to elem_2
-- 
-- The two arguments must be of the same type. If they are not,
-- you will receive "ERROR:  invalid input syntax ..."
BEGIN
  IF ((elem_1 IS NULL AND elem_2 IS NULL) OR elem_1 = elem_2) THEN
    RAISE EXCEPTION '% = %', elem_1, elem_2;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_not_equal(elem_1 anyelement, elem_2 anyelement) OWNER TO diogo;

--
-- Name: assert_raises(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_raises(call text) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Implicit errm, column version of assert_raises
BEGIN
  PERFORM test.assert_raises(call, NULL, NULL);
END;
$$;


ALTER FUNCTION test.assert_raises(call text) OWNER TO diogo;

--
-- Name: assert_raises(text, text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_raises(call text, errm text) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Implicit state version of assert_raises
BEGIN
  PERFORM test.assert_raises(call, errm, NULL);
END;
$$;


ALTER FUNCTION test.assert_raises(call text, errm text) OWNER TO diogo;

--
-- Name: assert_raises(text, text, text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_raises(call text, errm text, state text) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Raises an exception if call does not raise errm and/or state.
-- 
-- Example:
--
--    PERFORM test.assert_raises('get_transaction_by_id(''a'')', 'Bad argument', NULL);
--
-- If errm or state are NULL, that value will not be tested. This allows
-- you to test by message alone (since the 5-char SQLSTATE values are cryptic),
-- or trap a range of errors by SQLSTATE without regard for the exact message.
-- 
-- If you don't know the message you want to trap, call this function with
-- errm = '' and state = ''. The resultant error will tell you the
-- SQLSTATE and SQLERRM that were raised.
DECLARE
  msg       text;
BEGIN
  BEGIN
    EXECUTE test.statement(call);
  EXCEPTION
    WHEN OTHERS THEN
      IF ((state IS NOT NULL AND SQLSTATE != state) OR
          (errm IS NOT NULL AND SQLERRM != errm)) THEN
        msg = 'Call: ''' || call || ''' raised ''(' || SQLSTATE || ') ' || SQLERRM || ''' instead of ''(' || state || ') ' || errm || '''.';
        RAISE EXCEPTION '%', msg;
      END IF;
      RETURN;
  END;
  msg := 'Call: ' || quote_literal(call) || ' did not raise an error.';
  RAISE EXCEPTION '%', msg;
END;
$$;


ALTER FUNCTION test.assert_raises(call text, errm text, state text) OWNER TO diogo;

--
-- Name: assert_rows(text, text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_rows(call_1 text, call_2 text) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Asserts that two sets of rows have equal values.
--
-- Both arguments should be SELECT statements yielding a single row or a set of rows.
-- Either may also be any table, view, or procedure call that returns records.
-- It is also common for the second arg to be sans a FROM clause, and simply SELECT values.
-- Neither source nor expected need to be sorted. Either may include a trailing semicolon.
--
-- Example:
-- 
--    PERFORM test.assert_rows('SELECT first, last, city FROM table1',
--                             'SELECT ''Davy'', ''Crockett'', NULL');
DECLARE
  rec     record;
  s       text;
  e       text;
  msg     text;
BEGIN
  s := test.statement(call_1);
  e := test.statement(call_2);
  
  FOR rec in EXECUTE s || ' EXCEPT ' || e
  LOOP
    RAISE EXCEPTION 'Record: % from: % not found in: %', rec, call_1, call_2;
  END LOOP;
  
  FOR rec in EXECUTE e || ' EXCEPT ' || s
  LOOP
    RAISE EXCEPTION 'Record: % from: % not found in: %', rec, call_2, call_1;
  END LOOP;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_rows(call_1 text, call_2 text) OWNER TO diogo;

--
-- Name: assert_values(text, text, text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_values(call_1 text, call_2 text, columns text) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Raises an exception if SELECT columns FROM call_1 != SELECT columns FROM call_2.
--
-- Example:
--    row_1 := obj('get_favorite_user_ids(' || user_id || ')')
--    PERFORM test.assert_equal(row_1.object, 'users WHERE user_id = 355', 'last_name');
--
BEGIN
  PERFORM test.assert_rows(
    'SELECT ' || columns || ' FROM ' || call_1,
    'SELECT ' || columns || ' FROM ' || call_2
    );
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_values(call_1 text, call_2 text, columns text) OWNER TO diogo;

--
-- Name: assert_void(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION assert_void(call text) RETURNS void
    LANGUAGE plpgsql
    AS $$
-- Raises an exception if SELECT * FROM call != void.
DECLARE
  retval    text;
BEGIN
  EXECUTE test.statement(call) INTO retval;
  IF retval != '' THEN
    RAISE EXCEPTION 'Call: ''%'' did not return void. Got ''%'' instead.', call, retval;
  END IF;
  RETURN;
END;
$$;


ALTER FUNCTION test.assert_void(call text) OWNER TO diogo;

--
-- Name: attributes(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION attributes(tablename text) RETURNS SETOF pg_attribute
    LANGUAGE plpgsql
    AS $$
DECLARE
  rec      record;
  seen     int := 0;
  msg      text;
BEGIN
  FOR rec IN
    SELECT * FROM pg_attribute
    WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = tablename)
    -- Exclude system columns
    AND attnum >= 1
  LOOP
    seen := seen + 1;
    RETURN NEXT rec;
  END LOOP;
  
  IF seen = 0 THEN
    msg := quote_literal(tablename);
    RAISE EXCEPTION '% has no attributes.', msg;
  END IF;
END;
$$;


ALTER FUNCTION test.attributes(tablename text) OWNER TO diogo;

--
-- Name: constructor(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION constructor(tablename text) RETURNS text
    LANGUAGE sql
    AS $_$
-- Return the SQL statement used to construct the given global table.
  SELECT obj_description(pgc.oid, 'pg_class') FROM pg_class pgc WHERE relname = $1;
$_$;


ALTER FUNCTION test.constructor(tablename text) OWNER TO diogo;

--
-- Name: fail(); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION fail() RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Use this to finish a failed test. Raises exception '[FAIL]'.
BEGIN
  PERFORM test.finish('FAIL', NULL);
END;
$$;


ALTER FUNCTION test.fail() OWNER TO diogo;

--
-- Name: fail(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION fail(msg text) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Use this to finish a failed test. Raises exception '[FAIL] msg'.
BEGIN
  PERFORM test.finish('FAIL', msg);
END;
$$;


ALTER FUNCTION test.fail(msg text) OWNER TO diogo;

--
-- Name: finish(text, text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION finish(result text, msg text) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Use this to finish a test. Raises the given result as an exception (for rollback).
DECLARE
  fullmsg        text;
BEGIN
  fullmsg := '[' || result || ']';
  IF msg IS NOT NULL THEN
    fullmsg := fullmsg || ' ' || msg;
  END IF;
  RAISE EXCEPTION '%', fullmsg;
END;
$$;


ALTER FUNCTION test.finish(result text, msg text) OWNER TO diogo;

--
-- Name: get(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION get(p_tablename text) RETURNS record
    LANGUAGE plpgsql
    AS $$
DECLARE
  result         record;
BEGIN
  EXECUTE 'SELECT *, NULL::text AS __name__ FROM ' || p_tablename || ' LIMIT 1' INTO result;
  
  -- We add these values outside the EXECUTE in case the SELECT returned no rows.
  result.__name__ := p_tablename;
  
  RETURN result;
END;
$$;


ALTER FUNCTION test.get(p_tablename text) OWNER TO diogo;

--
-- Name: get(text, integer); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION get(p_tablename text, p_offset integer) RETURNS record
    LANGUAGE plpgsql
    AS $$
-- Returns a record (the first one, by default) from the given global table.
-- 
-- The returned record includes the following additional attributes:
--   * __name__ (text): The complete name of the TEMP table. This allows you
--       to pass g.__name__ to functions that take a 'call text' argument,
--       such as assert_column, assert_values, and assert_empty (since no
--       procedural languages support passing records as args).
DECLARE
  result         record;
  rownum         int;
BEGIN
  IF p_offset IS NULL OR p_offset < 0 THEN
    rownum := 0;
  ELSE
    rownum := p_offset;
  END IF;
  
  EXECUTE 'SELECT *, NULL::text AS __name__ FROM ' || p_tablename || ' LIMIT 1 OFFSET ' || rownum INTO result;
  
  -- We add these values outside the EXECUTE in case the SELECT returned no rows.
  result.__name__ := p_tablename;
  
  RETURN result;
END;
$$;


ALTER FUNCTION test.get(p_tablename text, p_offset integer) OWNER TO diogo;

--
-- Name: global(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION global(call text) RETURNS text
    LANGUAGE sql
    AS $_$
  SELECT global FROM test.global($1, NULL);
$_$;


ALTER FUNCTION test.global(call text) OWNER TO diogo;

--
-- Name: global(text, text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION global(call text, name text) RETURNS text
    LANGUAGE plpgsql
    AS $$
-- Stores the given call's output in a TEMP table, and returns the TEMP table name.
-- 
-- 'call' can be any SELECT, table, view, or procedure that returns records.
DECLARE
  tablename      text;
  creator        text;
BEGIN
  IF name IS NULL THEN
    tablename := '_global_' || nextval('test._global_ids');
  ELSE
    tablename := name;
  END IF;
  
  BEGIN
    EXECUTE 'DROP TABLE ' || tablename;
  EXCEPTION WHEN undefined_table THEN
    NULL;
  END;
  
  creator := test.statement(call);
  EXECUTE 'CREATE TEMP TABLE ' || tablename || ' WITHOUT OIDS AS ' || creator;
  EXECUTE 'COMMENT ON TABLE ' || tablename || ' IS ' || quote_literal(creator);
  RETURN tablename;
END;
$$;


ALTER FUNCTION test.global(call text, name text) OWNER TO diogo;

--
-- Name: iter(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION iter(tablename text) RETURNS text
    LANGUAGE sql
    AS $_$
-- Return SQL to retrieve all rows in the given table.
  SELECT 'SELECT * FROM ' || $1
$_$;


ALTER FUNCTION test.iter(tablename text) OWNER TO diogo;

--
-- Name: len(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION len(tablename text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
-- Return the number of rows in the given table.
DECLARE
  num    int;
BEGIN
  EXECUTE 'SELECT COUNT(*) FROM ' || tablename INTO num;
  RETURN num;
END
$$;


ALTER FUNCTION test.len(tablename text) OWNER TO diogo;

--
-- Name: pass(); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION pass() RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Use this to finish a successful test. Raises exception '[OK]'.
BEGIN
  PERFORM test.finish('OK', NULL);
END;
$$;


ALTER FUNCTION test.pass() OWNER TO diogo;

--
-- Name: pass(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION pass(msg text) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Use this to finish a successful test. Raises exception '[OK] msg'.
BEGIN
  PERFORM test.finish('OK', msg);
END;
$$;


ALTER FUNCTION test.pass(msg text) OWNER TO diogo;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: results; Type: TABLE; Schema: test; Owner: diogo; Tablespace: 
--

CREATE TABLE results (
    name text NOT NULL,
    module text,
    result text,
    errcode text,
    errmsg text,
    runtime timestamp with time zone DEFAULT now()
);


ALTER TABLE test.results OWNER TO diogo;

--
-- Name: run_all(); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION run_all() RETURNS SETOF results
    LANGUAGE plpgsql
    AS $$
-- Runs all known test functions, stores in test.results, and returns results.
DECLARE
  testname record;
  modulename record;
  output_record test.results%ROWTYPE;
BEGIN
  FOR modulename in SELECT DISTINCT module FROM test.testnames ORDER BY module ASC
  LOOP
    FOR testname IN SELECT name FROM test.testnames WHERE module = modulename.module ORDER BY name ASC
    LOOP
      SELECT INTO output_record * FROM test.run_test(testname.name);
      RETURN NEXT output_record;
    END LOOP;
  END LOOP;
  RETURN;
END;
$$;


ALTER FUNCTION test.run_all() OWNER TO diogo;

--
-- Name: run_module(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION run_module(modulename text) RETURNS SETOF results
    LANGUAGE plpgsql
    AS $$
-- Runs all tests in the given module, stores in test.results, and returns results.
DECLARE
  testname        record;
  output_record   test.results%ROWTYPE;
BEGIN
  FOR testname IN SELECT name FROM test.testnames WHERE module = modulename ORDER BY name ASC
  LOOP
    SELECT INTO output_record * FROM test.run_test(testname);
    RETURN NEXT output_record;
  END LOOP;
END;
$$;


ALTER FUNCTION test.run_module(modulename text) OWNER TO diogo;

--
-- Name: run_test(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION run_test(testname text) RETURNS results
    LANGUAGE plpgsql
    AS $$
-- Runs the named test, stores in test.results, and returns success.
DECLARE
  modulename      text;
  output_record   test.results%ROWTYPE;
  splitpoint      int;
BEGIN
  SELECT module INTO modulename FROM test.testnames WHERE name = testname;
  DELETE FROM test.results WHERE name = testname;
  
  BEGIN
    -- Allow test.* functions to be referenced without a schema name during this transaction.
    PERFORM set_config('search_path', 'test, ' || current_setting('search_path'), true);
    EXECUTE 'SELECT * FROM test.' || testname || '();' INTO output_record;
    RETURN output_record;
  EXCEPTION WHEN OTHERS THEN
    IF SQLSTATE = 'P0001' AND SQLERRM LIKE '[%]%' THEN
      splitpoint := position(']' in SQLERRM);
      INSERT INTO test.results (name, module, result, errcode, errmsg)
        VALUES (testname, modulename, substr(SQLERRM, 1, splitpoint),
                CASE WHEN SQLERRM LIKE '[FAIL]%' THEN SQLSTATE ELSE '' END,
                btrim(substr(SQLERRM, splitpoint + 1)));
      SELECT INTO output_record * FROM test.results WHERE name = testname;
      RETURN output_record;
    ELSE
      INSERT INTO test.results (name, module, result, errcode, errmsg)
        VALUES (testname, modulename, '[FAIL]', SQLSTATE, SQLERRM);
      SELECT INTO output_record * FROM test.results WHERE name = testname;
      RETURN output_record;
    END IF;
  END;
  
  RAISE EXCEPTION 'Test % did not raise an exception as it should have. Exceptions must ALWAYS be raised in test procedures for rollback.', testname;
END;
$$;


ALTER FUNCTION test.run_test(testname text) OWNER TO diogo;

--
-- Name: skip(); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION skip() RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Use this to skip a test. Raises exception '[SKIP]'.
BEGIN
  PERFORM test.finish('SKIP', NULL);
END;
$$;


ALTER FUNCTION test.skip() OWNER TO diogo;

--
-- Name: skip(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION skip(msg text) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Use this to skip a test. Raises exception '[SKIP] msg'.
BEGIN
  PERFORM test.finish('SKIP', msg);
END;
$$;


ALTER FUNCTION test.skip(msg text) OWNER TO diogo;

--
-- Name: statement(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION statement(call text) RETURNS text
    LANGUAGE plpgsql
    AS $$
-- Returns the given SQL string, prepending "SELECT * FROM " if missing.
DECLARE
  result    text;
BEGIN
  result := rtrim(call, ';');
  IF result ~* '^[[:space:]]*(SELECT|EXECUTE)[[:space:]]' THEN
    return result;
  ELSIF result ~* '^[[:space:]]*(VALUES)[[:space:]]*\\(' THEN
    return result;
  ELSE
    return 'SELECT * FROM ' || result;
  END IF;
END;
$$;


ALTER FUNCTION test.statement(call text) OWNER TO diogo;

--
-- Name: test_example(); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION test_example() RETURNS void
    LANGUAGE plpgsql
    AS $$
-- module: dojo
BEGIN
  -- ALWAYS RAISE EXCEPTION at the end of test procs to rollback!
  RAISE EXCEPTION '[OK]';
END;
$$;


ALTER FUNCTION test.test_example() OWNER TO diogo;

--
-- Name: timing(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION timing(call text) RETURNS interval
    LANGUAGE plpgsql
    AS $$
-- Return an interval encompassing 1,000 runs of the given call.
BEGIN
  RETURN test.timing(call, NULL);
END;
$$;


ALTER FUNCTION test.timing(call text) OWNER TO diogo;

--
-- Name: timing(text, integer); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION timing(call text, number integer) RETURNS interval
    LANGUAGE plpgsql
    AS $$
-- Return an interval encompassing 'number' runs of the given call.
-- If 'number' is NULL or omitted, it defaults to 1000000.
DECLARE
  v_call   text;
  v_number int;
  start    timestamp with time zone;
BEGIN
  v_call := test.statement(call);
  v_number := number;
  IF number IS NULL THEN v_number := 1000000; END IF;
  -- Mustn't use now() here since that value is fixed for the entire transaction.
  -- Also, clock_timestamp isn't available until 8.2 so we use timeofday instead.
  start := timeofday()::timestamp;
  FOR i IN 1..v_number
  LOOP
    EXECUTE v_call;
  END LOOP;
  -- We grab the total clock time outside the loop. It's a toss-up whether the loop
  -- overhead outweighs assignment overhead if we accumulated the time inside the loop;
  -- therefore I chose "outside" since it makes the whole run faster. ;)
  RETURN (timeofday()::timestamp - start);
END;
$$;


ALTER FUNCTION test.timing(call text, number integer) OWNER TO diogo;

--
-- Name: todo(); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION todo() RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Use this to abort a test as 'todo'. Raises exception '[TODO]'.
BEGIN
  PERFORM test.finish('TODO', NULL);
END;
$$;


ALTER FUNCTION test.todo() OWNER TO diogo;

--
-- Name: todo(text); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION todo(msg text) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
-- Use this to abort a test as 'todo'. Raises exception '[TODO] msg'.
BEGIN
  PERFORM test.finish('TODO', msg);
END;
$$;


ALTER FUNCTION test.todo(msg text) OWNER TO diogo;

--
-- Name: typename(anyelement); Type: FUNCTION; Schema: test; Owner: diogo
--

CREATE FUNCTION typename(elem anyelement) RETURNS text
    LANGUAGE plpgsql
    AS $$
-- Return the typename of the given element.
DECLARE
  name    text;
BEGIN
  CREATE TEMP TABLE _elem_type AS SELECT elem;
  SELECT INTO name pgt.typname
    FROM pg_attribute pga LEFT JOIN pg_type pgt ON pga.atttypid = pgt.oid
    WHERE pga.attrelid = (SELECT oid FROM pg_class WHERE relname = '_elem_type')
    AND pga.attnum = 1;
  DROP TABLE _elem_type;
  RETURN name;
END;
$$;


ALTER FUNCTION test.typename(elem anyelement) OWNER TO diogo;

--
-- Name: _global_ids; Type: SEQUENCE; Schema: test; Owner: diogo
--

CREATE SEQUENCE _global_ids
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE test._global_ids OWNER TO diogo;

--
-- Name: _global_ids; Type: SEQUENCE SET; Schema: test; Owner: diogo
--

SELECT pg_catalog.setval('_global_ids', 1, false);


--
-- Name: testnames; Type: VIEW; Schema: test; Owner: diogo
--

CREATE VIEW testnames AS
    SELECT pg_proc.proname AS name, "substring"(pg_proc.prosrc, '--\s+module[:]\s+(\S+)'::text) AS module FROM (pg_namespace LEFT JOIN pg_proc ON ((pg_proc.pronamespace = pg_namespace.oid))) WHERE ((pg_namespace.nspname = 'test'::name) AND (pg_proc.proname ~~ 'test_%'::text));


ALTER TABLE test.testnames OWNER TO diogo;

--
-- Data for Name: results; Type: TABLE DATA; Schema: test; Owner: diogo
--

COPY results (name, module, result, errcode, errmsg, runtime) FROM stdin;
test_soma_astro	dojo	[FAIL]	42883	function soma_astro(unknown) does not exist	2012-08-23 11:49:07.54123+00
test_word_numerology	dojo	[FAIL]	42883	function word_numerology(unknown) does not exist	2012-08-23 11:49:07.54123+00
test_example	dojo	[OK]			2012-08-23 12:11:45.593324+00
\.


--
-- Name: results_pkey; Type: CONSTRAINT; Schema: test; Owner: diogo; Tablespace: 
--

ALTER TABLE ONLY results
    ADD CONSTRAINT results_pkey PRIMARY KEY (name);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

