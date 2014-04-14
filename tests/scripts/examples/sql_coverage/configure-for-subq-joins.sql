<grammar.sql>

{_subqueryform |= "(select * from R1)"}
{_subqueryform |= "(select     _variable[#numone float],                  _variable[#arg numeric],             _variable[#string string]             from R1 order by __[#string] limit 1 offset 1)"}
{_subqueryform |= "(select   X._variable[#numone float]  __[#numone],   X._variable[#arg numeric]  __[#arg], X._variable[#string string] __[#string] from R1 X _jointype join R1 Y on X.ID = Y.ID where Y._numericcolumnpredicate)"}
{_subqueryform |= "(select max(_variable[#numone float]) __[#numone], sum(_variable[#arg numeric]) __[#arg],   _variable[#string string]             from R1 group by __[#string])"}

-- Reduce the explosions that result from too many options.
{_maybefloor |= " FLOOR "}
{_maybefloor |= " "}

{_countorsumormin |= "COUNT"}
{_countorsumormin |= "SUM"}
{_countorsumormin |= "MIN"}

-- Run the template against DDL with numeric types
{@aftermath = " + 2"} 
{@agg = "_countorsumormin"}
{@columnpredicate = "A._variable[#arg numeric] _cmp _numericvalue"}
{@columntype = "int"}
{@comparableconstant = "44"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[numeric] _cmp _value[int16]"}
{@dmltable = "_table"}
-- {@fromtables = "_subqueryform B, _table"}
{@fromtables = "_table B, _subqueryform "}
{@idcol = "ID"}
-- reducing the random values to int16 until overflow detection works
--{@insertvals = "_id, _value[string], _value[int32], _value[float]"}
{@insertvals = "_id, _value[string], _value[int16 null30], _value[float]"}
{@numcol = "NUM"}
{@onefun = "ABS"}
{@optionalfn = "_maybefloor"}
{@updatecolumn = "NUM"}
{@updatesource = "ID"}
{@updatevalue = "_value[int:0,100]"}

{@jointype = "_jointype"}
