env = require('test_run')
test_run = env.new()
engine = test_run:get_cfg('engine')

test_run:cmd("setopt delimiter ';'")

function string_hash(str)
if str == nil then return 0 end
local p = 27
local mod = 1000000000000
local result = 0
for i = 1, str:len() do
    result = (result * p + str:byte(i)) % mod
end
return result
end;

test_run:cmd("setopt delimiter ''");

-- Is supposed to be exported
normal = box.schema.space.create('normal', {engine = engine})
normal:format({                                     \
    {name = 'id', type = 'unsigned'},               \
    {name = 'string_field', type = 'string'},       \
    {name = 'integer_field', type = 'integer'},     \
    {name = 'number_field', type = 'number'},       \
    {name = 'double_field', type = 'double'},       \
    {name = 'boolean_field', type = 'boolean'}      \
})
_ = normal:create_index('primary', {  \
    type = 'tree',                    \
    parts = {'id'}                    \
})

normal:insert{1, 'str1', 540, 45324, 55546.67, true}
normal:insert{2, 'str2', 435324, 4532.3342, 566.674345677, true}
normal:insert{3, 'string3', 67545436, 45.5, 57546.7, true}
normal:insert{4, 'striiiiiiiiiiiing4', 111423, 45234, 56.7, false}
normal:insert{5, 'stringstring5', 8765, 9124678, 565.7, true}
normal:insert{6, 'str6', 8765, 453.543, 15554666.47, false}
normal:insert{7, 'str7', 98765789, 45324.5, 5612445.753, true}
normal:insert{8, 'str8', 456789, 45324, 56421.74, false}
normal:insert{9, 'str9', 251908, 7564.34, 56.7231, true}
normal:insert{10, 'str10', 543290780, 6435.735, 5141241.742, true}

-- Is supposed to be exported
no_tuples = box.schema.space.create('no_tuples', {engine = engine})
no_tuples:format({                                  \
    {name = 'id', type = 'unsigned'},               \
    {name = 'string_field', type = 'string'},       \
    {name = 'integer_field', type = 'integer'},     \
    {name = 'number_field', type = 'number'},       \
    {name = 'double_field', type = 'double'},       \
    {name = 'boolean_field', type = 'boolean'}      \
})
_ = no_tuples:create_index('primary', {  \
    type = 'tree',                       \
    parts = {'id'}                       \
})

-- Is not supposed to be exported
no_pk = box.schema.space.create('no_pk', {engine = engine})
no_pk:format({                                      \
    {name = 'id', type = 'unsigned'},               \
    {name = 'string_field', type = 'string'},       \
    {name = 'integer_field', type = 'integer'},     \
    {name = 'number_field', type = 'number'},       \
    {name = 'double_field', type = 'double'},       \
    {name = 'boolean_field', type = 'boolean'}      \
})

-- Is not supposed to be exported
space_scalar = box.schema.space.create('space_scalar', {engine = engine})
space_scalar:format({                       \
    {name = 'id', type = 'scalar'}          \
})
_ = space_scalar:create_index('primary', {  \
    type = 'tree',                          \
    parts = {'id'}                          \
})

-- Is not supposed to be exported
space_decimal = box.schema.space.create('space_decimal', {engine = engine})
space_decimal:format({                       \
    {name = 'id', type = 'decimal'}          \
})
_ = space_decimal:create_index('primary', {  \
    type = 'tree',                           \
    parts = {'id'}                           \
})

test_run:cmd("setopt delimiter ';'");
box.execute("CREATE TABLE ck_constraint_tester (
	less_than_ten INTEGER PRIMARY KEY,
	CHECK (less_than_ten < 10)
)");
box.execute("CREATE TABLE unique_not_null_tester (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	unique_field INTEGER UNIQUE,
	not_null_field INTEGER NOT NULL
)");
box.execute("CREATE TABLE default_stmt_tester (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	default_opt_field TEXT DEFAULT 'default text value'
)");
box.execute("CREATE TABLE fk_tester_parent (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	private_id INTEGER
)");
box.execute("CREATE TABLE fk_tester_son (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	foreign_id INTEGER,
	FOREIGN KEY (foreign_id) REFERENCES fk_tester_parent(id)
)");
box.execute("CREATE TABLE multifk_tester_parent (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	field1 INTEGER,
	field2 INTEGER,
	field3 INTEGER
)");
box.execute("CREATE UNIQUE INDEX fk_index
ON multifk_tester_parent (field1, field2, field3)");
box.execute("CREATE TABLE multifk_tester_son (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	son_field1 INTEGER,
	son_field2 INTEGER,
	son_field3 INTEGER,
	FOREIGN KEY (son_field1, son_field2, son_field3) REFERENCES multifk_tester_parent (field1, field2, field3)
)");
box.execute("CREATE VIEW view_tester AS
SELECT *
FROM unique_not_null_tester
CROSS JOIN ck_constraint_tester");

test_run:cmd("setopt delimiter ''");

box.ctl.export("test_db.sql")

box.execute("DROP VIEW view_tester")
box.execute("DROP TABLE multifk_tester_son")
box.execute("DROP INDEX fk_index ON multifk_tester_parent")
box.execute("DROP TABLE multifk_tester_parent")
box.execute("DROP TABLE fk_tester_son")
box.execute("DROP TABLE fk_tester_parent")
box.execute("DROP TABLE default_stmt_tester")
box.execute("DROP TABLE unique_not_null_tester")
box.execute("DROP TABLE ck_constraint_tester")
space_decimal:drop()
space_scalar:drop()
no_pk:drop()
no_tuples:drop()
normal:drop()

create_table_scripts = {}
create_view_scripts = {}
fk_scripts = {}
insert_list = {}
lines = {}
result = 0
mod = 2^40 + 1
i = 0
for line in io.lines("test_db.sql")                          \
do                                                           \
    lines[i] = line          \
    i = i + 1                                                \
    result = (result + string_hash(tostring(line))) % mod    \
end

for i = 0, 72                          \
do                                        \
    create_table_scripts[i + 1] = lines[i]    \
end
for i = 74, 78                          \
do                                        \
    fk_scripts[i - 73] = lines[i]    \
end
create_view_scripts[0] = lines[80]
for i = 81, 109                          \
do                                        \
    insert_list[i - 80] = lines[i]    \
end

--Uncomment to see a file
--lines

--Sort the scripts
table.sort(create_table_scripts)
table.sort(fk_scripts)
table.sort(create_view_scripts)
table.sort(insert_list)

--Show sorted scripts
create_table_scripts

create_view_scripts

fk_scripts

insert_list

result

