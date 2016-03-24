import itertools

def _getHtmlCheckConstraints(kernel, tblName):
	# check constraints
	rows = list(kernel.yieldQuery('''
SELECT r.conname, pg_catalog.pg_get_constraintdef(r.oid, true)
FROM pg_catalog.pg_constraint r
WHERE r.conrelid = %s::regclass AND r.contype = 'c'
ORDER BY 1;''', (tblName,)))
	if len(rows) > 0:
		yield '<h4>Check constraints:</h4><pre>'
		for row in rows:
			yield '  "{0}" {1}'.format(*row)
		yield '</pre>'


def _getHtmlIndexes(kernel, tblName):
	rows = list(kernel.yieldQuery('''
SELECT c2.relname, pg_catalog.pg_get_constraintdef(con.oid, true)
FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
WHERE c.oid = %s::regclass AND c.oid = i.indrelid AND i.indexrelid = c2.oid
ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
''', (tblName,)))
	if len(rows) > 0:
		yield '<h4>Indexes:</h4><pre>'
		for row in rows:
			yield '  "{0}" {1}'.format(*row)
		yield '</pre>'


def _getHtmlInheritance(kernel, tblName):
	# inheritance
	rows = kernel.yieldQuery('''
SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid=i.inhparent AND i.inhrelid = %s::regclass ORDER BY inhseqno;''', (tblName,))
	inh = ', '.join(row[0] for row in rows)
	if len(inh) > 0:
		yield '<b>Inherits</b>: <tt>{0}</tt>'.format(inh)


def _getHtmlTriggers(kernel, tblName):
	rows = list(kernel.yieldQuery('''
SELECT t.tgname, pg_catalog.pg_get_triggerdef(t.oid, true), t.tgenabled, t.tgisinternal
FROM pg_catalog.pg_trigger t
WHERE t.tgrelid = %s::regclass AND (NOT t.tgisinternal OR (t.tgisinternal AND t.tgenabled = 'D'))
ORDER BY 1''', (tblName,)))
	if len(rows) > 0:
		yield '<h4>Triggers:</h4><pre>'
		for row in rows:
			startPos = row[1].find(row[0]) # find trigger name in the 'CREATE TRIGGER' command and skip everything up until that point
			yield '  {0}'.format(row[1][startPos:])
		yield '</pre>'


def _printHtmlColumns(kernel, tblName):
	""" Print a HTML representation of a table's columns """
	kernel.printQuery("""
SELECT a.attname AS "Column", format_type(a.atttypid, a.atttypmod) AS "Type",
array_to_string(ARRAY[
	(CASE attnotnull WHEN true THEN 'not null' ELSE '' END),
	(CASE WHEN adsrc IS NOT NULL THEN 'default ' ELSE '' END) || adsrc
], ' ') AS "Modifiers"
FROM pg_attribute a LEFT JOIN pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)
WHERE attrelid = %s::regclass AND attnum > 0
""", params=(tblName,), rowCount=False)

def _printHtmlDetails(kernel, tblName):
	kernel.printData(html='\n'.join(itertools.chain(
		_getHtmlIndexes(kernel, tblName),
		# TODO add some more info (foreign keys, ...)
		_getHtmlCheckConstraints(kernel, tblName),
		_getHtmlTriggers(kernel, tblName),
		_getHtmlInheritance(kernel, tblName)
	)))
	

def inspectTable(kernel, name):
	# TODO only print table name if the table actually exists
	# TODO restructure this function to be a little more readable
	kernel.printData(html='<h3>Table {0}</h3>'.format(name))

	# List columns
	_printHtmlColumns(kernel, name)

	_printHtmlDetails(kernel, name)


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

	kernel.printQuery('''
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

	kernel.printQuery("""
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


