#!/usr/bin/python

import sqlite3
import re
import os       # We will want to use subprocess, but this works quickly
import time     # We need this for sleep
import getpass  # We use this to get the username

from sys import argv

def menu_pad( str, padchar, length ):
  return '{0:{fill}{align}{width}}'.format( str, fill=padchar, align='<', width=length)

def database_init():
  global datadir

  dbconn = sqlite3.connect( '.mim.sql3' )
  database_check( dbconn )

  c = dbconn.cursor()
  c.execute( "SELECT conf_value FROM db_conf WHERE id = 2")
  datadir = str(c.fetchone()[0])

  return dbconn

def database_check( dbconn ):
  c = dbconn.cursor()
  c.execute( "SELECT COUNT(*) AS total FROM sqlite_master WHERE type = 'table' AND name <> 'sqlite_sequence'" )
  tables_create = [
    '''CREATE TABLE databases ( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, path TEXT, db_conf INT )''',
    '''CREATE TABLE servers ( id INTEGER PRIMARY KEY, db_id INT, status TEXT, name TEXT )''',
    '''CREATE TABLE db_conf ( id INT, conf_key TEXT, conf_value TEXT )'''
  ]

  if c.fetchone()[0] != len(tables_create):
    # TODO: Do we need better database handling here? Check for which tables
    #       do exist? Only create the good ones? Delete all tables and start
    #       over? Who knows???

    print "Looks like a new install of MIM."
    print "Installing database..."

    datadir = raw_input( "What path should be used for data files? ")

    for table in tables_create:
      # print "Creating tables: %r" % table
      c.execute( table )

    tables_init = [
      # This is for the base my.cnf config
      '''INSERT INTO db_conf ( id ) VALUES ( 1 )''',
      # This is for the base data directory
      '''INSERT INTO db_conf ( id, conf_key, conf_value ) VALUES ( 2, 'Data Path', '%s' )''' % (datadir)
    ]


    for ins in tables_init:
      # print "Initializing tables: %r" % ins
      c.execute( ins )

    dbconn.commit()
    # Running the update here so we only have one copy of the conf. Easy!
    database_update_conf(dbconn)

# Need to make this better...
def database_update_conf(dbconn):
  c = dbconn.cursor()
  query = '''UPDATE db_conf SET conf_value = '
[mysqld_safe]
ledir=MIM_BASEDIR/bin

[mysqld]
user=%s
port=MIM_PORT
socket=MIM_SOCKPATH
datadir=MIM_DATADIR
basedir=MIM_BASEDIR
server-id=MIM_SERVERID
log-bin=mysql-bin
tmpdir=MIM_DATADIR
log_error=mysql.err

performance_schema=off

innodb_buffer_pool_size=64MB
innodb_log_file_size=64MB
sync_binlog=0
innodb_flush_log_at_trx_commit=2
skip-innodb_doublewrite
innodb_old_blocks_time=1000
innodb_purge_threads=1

' WHERE id = 1''' % (username)
  c.execute( query )
  dbconn.commit()

def database_get_next_server_id():
  global dbconn
  c = dbconn.cursor()
  #  This isn't so bad since it's not meant to be used concurrently
  c.execute( "SELECT MAX(id) FROM servers")
  # Being lazy here. If no rows, fetchone will be NoneType. Defaulting to 0.
  return str( int(c.fetchone()[0] or 0)+1 )

def menu_help():
  print '''
MySQL / MariaDB Instance Manager

Available Commands:

Database
=========

list db - print a list of configured databases
add db <db name> <db path> - Add a database
del db <#> - Remove a database
scan db <db path> - Scan path for databases

Servers
=======

list servers - prints a list of active servers
add server - Add Server Wizard
add server <db_id> <server_id> "<Name>"- add a server
del server <server_id> - delete a server
start <server_id> - start server
stop <server_id> - stop server
<server_id> - connect to a server
conf <editor_cmd> <server_id> - edit a config file with an editor
top <server_id> - Run innotop on the selected server
rename <server_id> "new name" - Rename the server to a new name
errorlog <server_id> - Show the error log

Advanced
========

adv remove binlog <server_id> - Remove the binlogs of a server, for resizing
updatedb - Update the default config

Misc
====
help - print this help page
mem - show system memory usage
quit - quit
'''

def menu_mem():
  print ""
  cmd = 'free -m'
  os.system(cmd)
  print ""

def menu_list_db():
  global dbconn
  c = dbconn.cursor()
  col1 = 3
  col2 = 30
  col3 = 30
  print '''
+--%s--+--%s--+--%s--+
|  \033[1m%s\033[0m  |  \033[1m%s\033[0m  |  \033[1m%s\033[0m  |
+--%s--+--%s--+--%s--+''' % (menu_pad('', '-', col1), menu_pad('', '-', col2), menu_pad('', '-', col3),
     menu_pad('ID', ' ', col1), menu_pad('Name', ' ', col2), menu_pad('Path', ' ', col3),
     menu_pad('', '-', col1), menu_pad('', '-', col2), menu_pad('', '-', col3)
    )
  for row in c.execute( 'SELECT id, name, path FROM databases ORDER BY id'):
    id, name, path = row
    print '''|  %s  |  %s  |  %s  |''' % (
      menu_pad(id, ' ', col1), menu_pad(name, ' ', col2), menu_pad(path, ' ', col3))
  print '''+--%s--+--%s--+--%s--+''' % (
    menu_pad('', '-', col1), menu_pad('', '-', col2), menu_pad('', '-', col3) )

def menu_add_db(args):
  global dbconn
  c = dbconn.cursor()
  c.execute( 'INSERT INTO databases ( name, path ) VALUES ( ?, ? )', args[0])
  dbconn.commit()
  print "\nAdded."

def menu_del_db(args):
  global dbconn
  c = dbconn.cursor()
  c.execute( 'DELETE FROM databases WHERE id = ?', args[0])
  dbconn.commit()
  print "\nDeleted."

def menu_scan_db(args):
  global dbconn
  counter = 0
  for dbdir in os.listdir(args[0]):
    dbname = dbdir
    dblocation = args[0]+"/"+dbdir;
    c = dbconn.cursor()
    c.execute( 'SELECT id FROM databases WHERE name = ? AND path = ?', [dbname, dblocation] )
    if c.fetchone():
      print "Database Exists: ( " + dbname + " )"
    else:
      print "Adding Database: ( " + dbname + " @ " + dblocation + ")"
      c.execute( 'INSERT INTO databases ( name, path ) VALUES ( ?, ? )', [dbname, dblocation])
      dbconn.commit()
      counter = counter + 1

  print "Found and added " + str(counter) + " databases."

def menu_list_server():
  global dbconn
  c = dbconn.cursor()
  col1 = 3
  col2 = 50
  col3 = 21
  col4 = 6
  print '''
+--%s--+--%s--+--%s--+--%s--+
|  \033[1m%s\033[0m  |  \033[1m%s\033[0m  |  \033[1m%s\033[0m  |  \033[1m%s\033[0m  |
+--%s--+--%s--+--%s--+--%s--+''' % (menu_pad('', '-', col1), menu_pad('', '-', col2), menu_pad('', '-', col3), menu_pad('', '-', col4),
     menu_pad('ID', ' ', col1), menu_pad('Name', ' ', col2), menu_pad('Database', ' ', col3), menu_pad('Status', ' ', col4),
     menu_pad('', '-', col1), menu_pad('', '-', col2), menu_pad('', '-', col3), menu_pad('', '-', col4)
    )
  for row in c.execute( 'SELECT s.id, s.name, d.name, s.status FROM servers AS s INNER JOIN databases AS d ON s.db_id = d.id ORDER BY s.id'):
    sid, sname, did, sstatus = row
    print '''|  %s  |  %s  |  %s  |  %s  |''' % (
      menu_pad(sid, ' ', col1), menu_pad(sname, ' ', col2), menu_pad(did, ' ', col3), menu_pad(sstatus, ' ', col4))
  print '''+--%s--+--%s--+--%s--+--%s--+''' % (
    menu_pad('', '-', col1), menu_pad('', '-', col2), menu_pad('', '-', col3), menu_pad('', '-', col4) )

def menu_add_server(args):
  global dbconn
  c = dbconn.cursor()

  try:
    c.execute( "INSERT INTO servers ( db_id, id, status, name ) VALUES ( ?, ?, 'OFF', ? )", args[0])
    dbconn.commit()
    create_server( args[0][0], args[0][1])
    print "\nAdded."
  except sqlite3.IntegrityError:
    print "\nID already exists!"

def menu_del_server(args):
  global dbconn
  c = dbconn.cursor()
  c.execute( 'DELETE FROM servers WHERE id = ?', args )
  dbconn.commit()
  # This is really dangerous... what if the arg wasn't passed? It'll delete everything!
  cmd = 'rm -rf '+datadir+'/'+args[0]
  os.system(cmd)
  print "\nDeleted."

def menu_rename_server(args):
  global dbconn
  c = dbconn.cursor()
  c.execute( "SELECT name FROM servers WHERE id = ?", (args[0][0],) )
  old_server_name = c.fetchone()[0]
  c.execute( 'UPDATE servers SET name = ? WHERE id = ?', (args[0][1], args[0][0]) )
  print "\nRenamed %s to %s." % (old_server_name, args[0][1])

def menu_wizard_add_server():
  menu_list_db()
  wizardas_db_id = raw_input( "Which database would you like to use? ")
  menu_list_server()
  wizardas_server_id = raw_input( "Which server ID would you like to use (leave blank for next ID)? ")
  try:
    # Check to see if it's a number. If not, run the function to grab the next ID
    int(wizardas_server_id)
  except:
    wizardas_server_id = database_get_next_server_id()
    print "Using server ID: %s" % wizardas_server_id
  wizardas_server_name = raw_input( "\nWhat do you want to name this server? ")
  menu_add_server( [(wizardas_db_id, wizardas_server_id, wizardas_server_name)] )

def create_server( database_id, server_id ):
  global dbconn
  c = dbconn.cursor()
  c.execute( "SELECT path FROM databases WHERE id = ?", database_id)
  bindir = c.fetchone()[0]
  # This is really dangerous... what if the arg wasn't passed? It'll delete everything!
  cmd = 'rm -rf '+datadir+'/'+server_id
  os.system(cmd)
  cmd = 'PATH=$PATH:' + bindir + '/bin ' + bindir + '/scripts/mysql_install_db --basedir=' + bindir + ' --datadir=' + datadir + '/' + server_id + ' --user=' + username
  os.system(cmd)
  c.execute( "SELECT conf_value FROM db_conf WHERE id = 1")
  myconfig = c.fetchone()[0]
  myconfig = myconfig.replace( 'MIM_PORT', str(33000+int(server_id)))
  myconfig = myconfig.replace( 'MIM_SOCKPATH', datadir+'/'+server_id+'/mysql.sock')
  myconfig = myconfig.replace( 'MIM_DATADIR', datadir+'/'+server_id)
  myconfig = myconfig.replace( 'MIM_BASEDIR', bindir)
  myconfig = myconfig.replace( 'MIM_SERVERID', server_id)
  config_filename = datadir +'/'+server_id+'/my.cnf'
  config_file = open(config_filename, 'w+')
  config_file.write(myconfig)
  config_file.close()

def menu_start_server(args):
  global dbconn
  c = dbconn.cursor()
  c.execute( "UPDATE servers SET status = 'ON' WHERE id = ?", args)
  dbconn.commit()
  c.execute( "SELECT d.path FROM databases AS d INNER JOIN servers AS s ON d.id = s.db_id WHERE s.id = ?", args)
  bindir = c.fetchone()[0]
  server_id = args[0]
  cmd = 'PATH=$PATH:'+bindir+'/bin '+bindir+'/bin/mysqld_safe --defaults-file='+datadir+'/'+server_id+'/my.cnf &'
  os.system(cmd)
  time.sleep(2) # TODO: Make this more intelligent
  print "\nStarted."

def menu_stop_server(args):
  global dbconn
  c = dbconn.cursor()
  c.execute( "UPDATE servers SET status = 'OFF' WHERE id = ?", args)
  dbconn.commit()
  c.execute( "SELECT d.path FROM databases AS d INNER JOIN servers AS s ON d.id = s.db_id WHERE s.id = ?", args)
  bindir = c.fetchone()[0]
  server_id = args[0]
  cmd = 'PATH=$PATH:'+bindir+'/bin '+bindir+'/bin/mysqladmin --socket='+datadir+'/'+server_id+'/mysql.sock --user=root shutdown'
  os.system(cmd)
  print "\nStopped."

def menu_connect_server(args):
  global dbconn
  c = dbconn.cursor()
  c.execute( "SELECT d.path FROM databases AS d INNER JOIN servers AS s ON d.id = s.db_id WHERE s.id = ?", args)
  bindir = c.fetchone()[0]
  server_id = args[0]
  cmd = bindir+'/bin/mysql --socket='+datadir+'/'+server_id+'/mysql.sock --user=root'
  os.system(cmd)

def menu_edit_server(args):
  editor_cmd, server_id = args[0]
  cmd = editor_cmd+' '+datadir+'/'+server_id+'/my.cnf'
  os.system(cmd)

def menu_top_server(args):
  server_id = args[0]
  cmd = 'innotop --user=root --socket='+datadir+'/'+server_id+'/mysql.sock'
  os.system(cmd)

def menu_errorlog(args):
  server_id = args[0]
  cmd = 'less '+datadir+'/'+server_id+'/mysql.err'
  os.system(cmd)


def adv_rm_binlog(args):
  menu_stop_server(args)
  server_id = args[0]
  cmd = 'rm -rf {}/{}/ib_logfile*'.format( datadir, server_id )
  os.system(cmd)

# Initalization
username = getpass.getuser()
dbconn = database_init()
action = 'list servers'

print "Datadir: %s" % (datadir)

# TODO:
# Check conf directory
# Check data directory


# Infinite Loop

while action != 'quit':

  if action == '':
    # Make a default if someone hits enter
    action = 'list servers'

  if action == 'help':
    menu_help()

  if action == 'mem':
    menu_mem()

  if action == 'updatedb':
    database_update_conf(dbconn)

  if action == 'list db':
    menu_list_db()

  cmd = re.findall( '^add db (\S+) (\S+)$', action)
  if cmd:
    menu_add_db(cmd)

  cmd = re.findall( '^del db (\d+)$', action)
  if cmd:
    menu_del_db(cmd)

  cmd = re.findall( '^scan db (\S+)$', action)
  if cmd:
    menu_scan_db(cmd)

  if action == 'list servers':
    menu_list_server()

  if action == 'add server':
    menu_wizard_add_server()

  cmd = re.findall( '^add server (\d+) (\d+) "(.+)"$', action)
  if cmd:
    menu_add_server(cmd)

  cmd = re.findall( '^del server (\d+)$', action)
  if cmd:
    menu_del_server(cmd)

  cmd = re.findall( '^start (\d+)$', action)
  if cmd:
    menu_start_server(cmd)

  cmd = re.findall( '^stop (\d+)$', action)
  if cmd:
    menu_stop_server(cmd)

  cmd = re.findall( '^(\d+)$', action)
  if cmd:
    menu_connect_server(cmd)

  cmd = re.findall( '^conf (\S+) (\d+)$', action)
  if cmd:
    menu_edit_server(cmd)

  cmd = re.findall( '^top (\d+)$', action)
  if cmd:
    menu_top_server(cmd)

  cmd = re.findall( '^rename (\d+) "(.+)"$', action)
  if cmd:
    menu_rename_server(cmd)

  cmd = re.findall( '^errorlog (\d+)$', action)
  if cmd:
    menu_errorlog(cmd)


# Advanced Commands

  cmd = re.findall( '^adv remove binlog (\d+)$', action)
  if cmd:
    adv_rm_binlog(cmd)


  print "Type 'help' for help."
  action = raw_input( "Yes, me lord? ")


print "Bye!"
