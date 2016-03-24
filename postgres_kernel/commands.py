
def inspectTable(kernel, name):
	# TODO add some more info (not null, defaults, indexes, foreign keys, other constraints, inheritance)
	kernel._runQuery("""SELECT a.attname AS "Column", format_type(a.atttypid, a.atttypmod) AS "Type"
		FROM pg_attribute a WHERE attrelid = %s::regclass AND attnum >= 0;""", params=(name,))

def listObjects(kernel, args, details, types=('r','v','m','S','f','')):
	params = set()

	# shamelessly copied from `psql -E`
	detailStr = ''
	if details == True:
		detailStr = ''',
pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "Size",
pg_catalog.obj_description(c.oid, 'pg_class') as "Description"'''

	if len(args) == 0:
		schemaStr = """
AND n.nspname <> 'pg_catalog'
AND n.nspname <> 'information_schema'
AND n.nspname !~ '^pg_toast'
AND pg_catalog.pg_table_is_visible(c.oid)"""
	elif len(args) == 1:
		schemaStr = """ AND n.nspname ~ %s """
		params = ('^({0})$'.format(args[0]),)
	else:
		raise Exception("Too many arguments!")

	kernel._runQuery('''
SELECT n.nspname as "Schema",
c.relname as "Name",
CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' END as "Type",
pg_catalog.pg_get_userbyid(c.relowner) as "Owner"{details}
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN {types}
{schemas}
ORDER BY 1,2'''.format(types=types, details=detailStr, schemas=schemaStr), params=params)

def listSchemas(kernel, details):
	detailStr = ''
	if details:
		detailStr = """,
pg_catalog.array_to_string(n.nspacl, E'\n') AS "Access privileges",
pg_catalog.obj_description(n.oid, 'pg_namespace') AS "Description" """

	kernel._runQuery("""
SELECT n.nspname AS "Name",
pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"{details}
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
ORDER BY 1;""".format(details=detailStr))

def parse(kernel, cmd, silent):
	# strip initial backslash and split by spaces
	assert cmd.startswith('\\')
	cmd = cmd[1:].split(' ')
	args = cmd[1:]
	cmd = cmd[0]

	# details?
	details = False
	if cmd.endswith('+'):
		details = True
		cmd = cmd[:-1]

	# listObject commands:
	listCmds = {
		'd': ('r','v','m','S','f',''), # list most of them \d
		'di': ('i', ''), # list indexes
		'dm': ('m', ''), # list materialized views
		'ds': ('s', ''), # list sequences
		'dt': ('r', ''), # list tables
		'dv': ('v', '')  # list views
	}

	if cmd in ['c', 'connect']:
		kernel.connect(args)
	elif cmd == 'conninfo':
		kernel.connectionInfo()
	elif cmd == 'd' and len(args) == 1:
		inspectTable(kernel, args[0])
	elif cmd in listCmds:
		listObjects(kernel, args, details, listCmds[cmd])
	elif cmd == 'dn':
		listSchemas(kernel, details)
	else:
		# TODO use a more specific exception type
		raise Exception("Unknown command: '{0}'".format(cmd))


