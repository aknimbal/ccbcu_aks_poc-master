import pypyodbc
import logging
import time

__version__ = "0.1.0"


initial_sql = """IF  NOT EXISTS (SELECT * FROM sys.tables
WHERE name = N'Log' AND type = 'U')
	CREATE TABLE Log(
                    [id] [int] IDENTITY(1,1) NOT NULL,
                    TimeStamp datetimeoffset(7),
                    Source TEXT,
                    LogLevel INT,
                    LogLevelName varchar(50),
                    Message TEXT,
                    Args TEXT,
                    Module TEXT,
                    FuncName TEXT,
                    [LineNo] INT,
                    Exception TEXT,
                    Process INT,
                    Thread TEXT,
                    ThreadName TEXT
               )"""

insertion_sql = """INSERT INTO Log(
                    TimeStamp,
                    Source,
                    LogLevel,
                    LogLevelName,
                    Message,
                    Args,
                    Module,
                    FuncName,
                    [LineNo],
                    Exception,
                    Process,
                    Thread,
                    ThreadName
               )
               VALUES (
                    '%(dbtime)s',
                    '%(name)s',
                    %(levelno)d,
                    '%(levelname)s',
                    '%(msg)s',
                    '%(args)s',
                    '%(module)s',
                    '%(funcName)s',
                    %(lineno)d,
                    '%(exc_text)s',
                    %(process)d,
                    '%(thread)s',
                    '%(threadName)s'
               );
               """


class SQLHandler(logging.Handler):

    def __init__(self, host, port, user, passwd, database):
        logging.Handler.__init__(self)

        print("init SQLHandler")
        self.host= host
        self.port= port
        self.user= user
        self.passwd= passwd
        self.database= database

        connection_string ='Driver={{SQL Server Native Client 11.0}};Server={0};Database={1};Uid={2};Pwd={3};'.format(host, database, user, passwd)
        self.conn = pypyodbc.connect(connection_string, autocommit=True)

        cursor = self.conn.cursor()
        cursor.execute(initial_sql)
        cursor.close()
        print("init end SQLHandler")

    def format_time(self, record):
        """
        Create a time stamp
        """
        record.dbtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))

    def format_msg(self, record):
        record.msg = record.msg.replace("\'", "''")
        record.msg = record.msg.replace(" ' ", " '' ")
        record.msg = record.msg.replace("'", "''")
        record.msg = record.msg.strip("'")   

    def format_exc_text(self, record):
        if record.exc_text:
            record.exc_text = record.exc_text.replace("\'", "''")
            record.exc_text = record.exc_text.replace(" ' ", " '' ")
            record.exc_text = record.exc_text.replace("'", "''")
            record.exc_text = record.exc_text.strip("'") 

    def emit(self, record):
        #print("start emit")

        self.format(record)
        self.format_time(record)
        self.format_msg(record)

        if record.exc_info:  # for exceptions
            record.exc_text = logging._defaultFormatter.formatException(record.exc_info)
        else:
            record.exc_text = ""

        self.format_exc_text(record)

        # Insert the log record
        sql = insertion_sql % record.__dict__

        cursor = self.conn.cursor()
        #print(f'sql: {sql}')
        cursor.execute(sql)
        cursor.close()
        #print("end emit")
