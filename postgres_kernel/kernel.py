from postgres_kernel import commands

from ipykernel.kernelbase import Kernel
from ipython_genutils import py3compat
from IPython import display
import getpass
import logging
import psycopg2
import traceback
import sys
import time

class PostgresKernel(Kernel):
	implementation = 'PostgreSQL'
	implementation_version = '0.1'
	language = 'postgres'
	language_version = '0.1'
	language_info = {
		'mimetype': 'text/x-postgresql',
		'name': 'sql'
	}
	banner = "PostgreSQL kernel (using psycopg2 to connect to your Postgres database)"

	def __init__(self, *args, **kwargs):
		super(PostgresKernel, self).__init__(*args, **kwargs)
		# TODO find a way to let users configure the psql settings
		self.conn = None

	def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
		rc = {
			'status': 'ok',
			# The base class increments the execution count
			'execution_count': self.execution_count,
			'payload': [],
			'user_expressions': {}
		}

		try:
			if code.startswith('\\'):
				commands.parse(self, code, silent)
			else:
				self._runQuery(code, silent)

		except:
			# TODO don't print a traceback for simple postgres errors
			etype, evalue, tb = sys.exc_info()
			tb_list = traceback.format_exception(etype, evalue, tb)
			rc['status'] = 'error'
			self.send_response(self.iopub_socket, 'error', self._formatException(etype, evalue, tb_list))
			logging.error(evalue)

		return rc

	def connect(self, args):
		if self.conn != None:
			self.conn.close()
			self.conn = None

		values = {
			'host': 'localhost',
			'port': 5432,
			'user': 'postgres'
		}

		# parse args
		kwargs = {}
		flags = set()
		nopwd = False
		for k,v in ((arg.split('=',1)+[None])[:2] for arg in args):
			if v != None:
				kwargs[k] = v
			else: flags.add(k)

		# parse flags
		for flag in flags:
			if flag == 'nopassword':
				if 'password' in kwargs:
					raise Exception("you specified both a password and the nopassword flag!")
				nopwd = True
			else:
				raise Exception("Unsupported flag (only 'nopassword' is supported right now)!")

		# ask for a password if necessary
		if not 'password' in kwargs and not nopwd:
			kwargs['password'] = getpass.getpass('Database password: ')

		values.update(kwargs)

		self.conn = psycopg2.connect(**values)
		self.conn.autocommit = True # let the user handle transactions explicitly
		self.connInfo = kwargs

		self.send_response(self.iopub_socket, 'stream', {'name': 'stdout', 'text': 'ok'})


	def connectionInfo(self):
		if self.conn == None:
			msg = '-- not connected (use \connect to initiate a connection) --'
		else:
			msg = ', '.join('{0}={1}'.format(k,v) for k,v in self.connInfo.items())
		self.send_response(self.iopub_socket, 'stream', {'name': 'stdout', 'text': msg})


	def _formatDuration(self, duration):
		""" Takes a integer or floating point duration in seconds and converts it to
		a human readable string of the format '1d 2h 3m 4s/5ms' (leaving out 0 values
		and only printing seconds OR milliseconds) """
		negative = duration < 0
		rc = []

		if negative:
			duration = abs(duration)

		# secs
		duration, secs = divmod(duration, 60)
		if secs >= 1:
			# format: 1.123s
			rc.append('{0:1.3f}s'.format(secs))
		else:
			# format: 123.45ms
			rc.append('{0:1.2f}ms'.format(secs/1000))

		# mins
		duration, mins = divmod(duration, 60)
		if mins > 0:
			rc.append('{0}m').format(mins)

		# hours
		duration, hours = divmod(duration, 24)
		if hours > 0:
			rc.append('{0}h').format(hours)

		# days
		if duration > 0:
			rc.append('{0}d').format(duration)

		rc.reverse()
		rc = ' '.join(rc)

		if negative:
			rc = '-'+rc

		return rc

	def _formatException(self, etype, evalue, tb):
		rc = {
			'status': 'error',
			'execution_count': self.execution_count,
			'traceback': tb,
			'ename': str(etype.__name__),
			'evalue': py3compat.safe_unicode(evalue)
		}
		return rc

	def _runQuery(self, query, silent=False, params=None):
		if self.conn == None:
			# connect to the default database
			self.connect(['nopassword'])

		#logging.debug('--Q: {0}'.format(query))
		#logging.debug(' -p: {0}'.format(params))
		with self.conn.cursor() as cur: 
			startTime = time.time()
			cur.execute(query, params)
			duration = time.time() - startTime

			self.lastQueryDuration = duration
			self.lastQueryDurationFormatted = self._formatDuration(duration)

			if not silent:
				self._sendResultTable(cur)

	def _sendResultTable(self, cur):
		""" Sends a query result as HTML table. If there is none, only the result count will be displayed """
		if cur.description != None:
			# we've got results => print them as a table
			headers = []
			for col in cur.description:
				headers.append('<th>{0}</th>'.format(col.name))
			data = []
			for row in cur:
				colData = []
				for col in row:
					colData.append('<td>{0}</td>'.format(col))
				data.append('<tr>{0}</tr>'.format(''.join(colData)))
			html = '<table><tr>{0}</tr>{1}</table>'.format(''.join(headers), ''.join(data))

			data = {'source': 'psql', 'data': {'text/html': html}}
			self.send_response(self.iopub_socket, 'display_data', data)
		# print row count
		if cur.rowcount >= 0:
			text = '{0} rows'.format(cur.rowcount)
		else:
			text = 'ok' # begin, rollback, ... will set the row count to -1

		text += ' (took {0})'.format(self.lastQueryDurationFormatted)
		self.send_response(self.iopub_socket, 'stream', {'name': 'stdout', 'text': text})


if __name__ == '__main__':
	from ipykernel.kernelapp import IPKernelApp
	IPKernelApp.launch_instance(kernel_class=EchoKernel)
