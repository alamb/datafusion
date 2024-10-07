<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

<!---
This file was generated by the dev/update_function_docs.sh script.
Do not edit it manually as changes will be overwritten.
Instead, edit the ScalarUDFImpl's documentation() function to
update documentation for an individual UDF or the
dev/update_function_docs.sh file for updating surrounding text.
-->

# Scalar Functions (NEW)

Note: this documentation is in the process of being migrated to be [automatically created from the codebase].
Please see the [Scalar Functions (old)](aggregate_functions.md) page for
the rest of the documentation.

[automatically created from the codebase]: https://github.com/apache/datafusion/issues/12740

## Math Functions

- [log](#log)

### `log`

Returns the base-x logarithm of a number. Can either provide a specified base, or if omitted then takes the base-10 of a number.

```
log(base, numeric_expression)
log(numeric_expression)
```

#### Arguments

- **base**: Base numeric expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **numeric_expression**: Numeric expression to operate on. Can be a constant, column, or function, and any combination of operators.

## Conditional Functions

- [coalesce](#coalesce)

### `coalesce`

Returns the first of its arguments that is not _null_. Returns _null_ if all arguments are _null_. This function is often used to substitute a default value for _null_ values.

```
coalesce(expression1[, ..., expression_n])
```

#### Arguments

- **expression1, expression_n**: Expression to use if previous expressions are _null_. Can be a constant, column, or function, and any combination of arithmetic operators. Pass as many expression arguments as necessary.

## String Functions

- [ascii](#ascii)
- [rpad](#rpad)

### `ascii`

Returns the ASCII value of the first character in a string.

```
ascii(str)
```

#### Arguments

- **str**: String expression to operate on. Can be a constant, column, or function that evaluates to or can be coerced to a Utf8, LargeUtf8 or a Utf8View.

**Related functions**:

- [chr](#chr)

### `rpad`

Pads the right side of a string with another string to a specified string length.

```
rpad(str, n[, padding_str])
```

#### Arguments

- **str**: String expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **n**: String length to pad to.
- **padding_str**: String expression to pad with. Can be a constant, column, or function, and any combination of string operators. _Default is a space._

**Related functions**:

- [lpad](#lpad)

## Binary String Functions

- [decode](#decode)
- [encode](#encode)

### `decode`

Decode binary data from textual representation in string.

```
decode(expression, format)
```

#### Arguments

- **expression**: Expression containing encoded string data
- **format**: Same arguments as [encode](#encode)

**Related functions**:

- [encode](#encode)

### `encode`

Encode binary data into a textual representation.

```
encode(expression, format)
```

#### Arguments

- **expression**: Expression containing string or binary data
- **format**: Supported formats are: `base64`, `hex`

**Related functions**:

- [decode](#decode)

## Regular Expression Functions

Apache DataFusion uses a [PCRE-like](https://en.wikibooks.org/wiki/Regular_Expressions/Perl-Compatible_Regular_Expressions)
regular expression [syntax](https://docs.rs/regex/latest/regex/#syntax)
(minus support for several features including look-around and backreferences).
The following regular expression functions are supported:

- [regexp_like](#regexp_like)

### `regexp_like`

Returns true if a [regular expression](https://docs.rs/regex/latest/regex/#syntax) has at least one match in a string, false otherwise.

```
regexp_like(str, regexp[, flags])
```

#### Arguments

- **str**: String expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **regexp**: Regular expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **flags**: Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?

#### Example

```sql
select regexp_like('Köln', '[a-zA-Z]ö[a-zA-Z]{2}');
+--------------------------------------------------------+
| regexp_like(Utf8("Köln"),Utf8("[a-zA-Z]ö[a-zA-Z]{2}")) |
+--------------------------------------------------------+
| true                                                   |
+--------------------------------------------------------+
SELECT regexp_like('aBc', '(b|d)', 'i');
+--------------------------------------------------+
| regexp_like(Utf8("aBc"),Utf8("(b|d)"),Utf8("i")) |
+--------------------------------------------------+
| true                                             |
+--------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/regexp.rs)

## Time and Date Functions

- [to_date](#to_date)

### `to_date`

Converts a value to a date (`YYYY-MM-DD`).
Supports strings, integer and double types as input.
Strings are parsed as YYYY-MM-DD (e.g. '2023-07-20') if no [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)s are provided.
Integers and doubles are interpreted as days since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding date.

Note: `to_date` returns Date32, which represents its values as the number of days since unix epoch(`1970-01-01`) stored as signed 32 bit value. The largest supported date value is `9999-12-31`.

```
to_date('2017-05-31', '%Y-%m-%d')
```

#### Arguments

- **expression**: String expression to operate on. Can be a constant, column, or function, and any combination of operators.
- **format_n**: Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```sql
> select to_date('2023-01-31');
+-----------------------------+
| to_date(Utf8("2023-01-31")) |
+-----------------------------+
| 2023-01-31                  |
+-----------------------------+
> select to_date('2023/01/31', '%Y-%m-%d', '%Y/%m/%d');
+---------------------------------------------------------------+
| to_date(Utf8("2023/01/31"),Utf8("%Y-%m-%d"),Utf8("%Y/%m/%d")) |
+---------------------------------------------------------------+
| 2023-01-31                                                    |
+---------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_date.rs)

## Hashing Functions

- [sha224](#sha224)

### `sha224`

Computes the SHA-224 hash of a binary string.

```
sha224(expression)
```

#### Arguments

- **expression**: String expression to operate on. Can be a constant, column, or function, and any combination of operators.