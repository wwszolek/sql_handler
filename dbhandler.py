import mysql.connector
import sys
from datetime import datetime, timedelta

def prepare_args(args):
    if isinstance(args,str):
        return args.strip('\'')
    elif isinstance(args,list):
        prepared = [prepare_args(x) for x in args]
        for x in prepared:
            if x is None:
                return prepared
        return None
    elif args is None:
        return ''
    else:
        raise TypeError


def _prepare_conditions(logic='and', wildcard=True, update=False, **conditions):
    operators=('=','<','>','!','^')
    keywords=('and', 'or')
    template='`%s`%s%s'
    wildcards=('%','_')
    def null_check(op,val):
        if op=='=' and val.lower()=='null':
            return (' IS ','NULL')
        elif op=='!=' and val.lower()=='null':
            return (' IS NOT ','NULL')
        else:
            return (op,val)
            
    def wildcard_check(op,val,wildcard):
        if op=='=' and wildcard==True:
            for w in wildcards:
                if w in val:
                    return (' LIKE ',val)
            return (op,val)
        elif op=='!=' and wildcard==True:
            for w in wildcards:
                if w in val:
                    return (' NOT LIKE ',val)
            return (op,val)
        elif wildcard==False:
            return (op,val)
        else:
            return (op,val.strip(''.join(wildcards)))

    if len(conditions)>0:
        if logic.lower() not in keywords:
            raise ValueError('logic flag should be \'or\' or \'and\'')

        st=[]
        updates=[]
        print(conditions)
        for field,condition in conditions.items():
            format=''
            op=''
            val=''
            tokens=[]
            values=[]
            if isinstance(condition,tuple):
                tokens=condition[0].split(' ')
                for c in condition[1:]:
                    if type(c)==type(datetime.now()):
                        values.append('NOW()')
                    else:
                        values.append(str(c))
            else:
                tokens=condition.split(' ')
                values=None

            for t in tokens:
                if t not in keywords:
                    for c in t:
                        if c in operators:
                            op+=c
                        elif c.isalnum() or c=='-' or c in wildcards:
                            val+=c

                    if t is tokens[-1]:
                        val=values.pop(0) if len(val)==0 else val
                        op,val=wildcard_check(op,val,wildcard)
                        op,val=null_check(op,val)
                        val='\'%s\''%val if val not in ('NULL','NOW()') else val
                        
                        if update and op=='^':
                            updates.append(template%(field,'=',val))
                        else:
                            format+=template%(field,op,val)

                elif t is not tokens[0]:
                    val=values.pop(0) if len(val)==0 else val
                    op,val=wildcard_check(op,val,wildcard)
                    op,val=null_check(op,val)
                    val='\'%s\''%val if val not in ('NULL','NOW()') else val

                    if update and op=='^':
                        updates.append(template%(field,'=',val))
                    else:
                        format+=template%(field,op,val)+' '+t.upper()+' '
                    op=''
                    val=''
                
            st.append('('+format+')')
        if st[-1]=='()':
            st.pop(-1)
        ret=(' '+logic.upper()+' ').join(st) if len(st)>0 else '1=1'
        if update:
            return updates,ret
        else:
            return ret
    else:
        if update:
            return None,'1=1'
        else:
            return '1=1'

class DBHandler():
    '''mysql database handler'''
    field_parameters={
        'field':None,
        'type':{int:'INT', str:'VARCHAR', datetime:'TIMESTAMP'},
        'default_size':{int:11, str:50},
        'size':None,
        'null':{True:'NULL', False:'NOT NULL'},
        'unique':{True:'UNIQUE', False:''},
        'primary':{True:'PRIMARY KEY', False:''},
        'default':{False:'DEFAULT NULL', True:'DEFAULT \'%s\''},
        'auto_increment':{True:'AUTO_INCREMENT', False:''}
        }
    _config={
        'host':'localhost',
        'port':'',
        'user':'',
        'password':'',
        'database':''
        }
    _connection=mysql.connector.MySQLConnection()
    _timezone=timedelta(hours=0)
    
    def __init__(self,**kwconfig):
        self._update_config(**kwconfig)
        self.connect()

    def __del__(self):
        self._connection.close()

    def _update_config(self,**kwconfig):
        '''update _config only with exisiting keys'''
        if len(kwconfig)>0:
            for k in kwconfig.keys():
                if k in self._config.keys():
                    self._config[k] = kwconfig[k]
        else:
            raise TypeError('Empty config')

    def isconnected(self):
        return self._connection.is_connected()

    def connect(self):
        '''connects to mysql database'''
        if self.isconnected():
            self._connection.close()

        try:
            print('connecting to %s' % self._config['database'])
            self._connection=mysql.connector.connect(**self._config)
        except mysql.connector.errors.InterfaceError:
            print('failed to connect to mysql server\n %s' % sys.exc_info()[1])

        else:
            print('succes')
        
    def list_tables(self):
        '''returns list of names of all tables'''
        if self.isconnected():
            try:
                cursor=self._connection.cursor()
                cursor.execute('SHOW TABLES;')
                return [t[0] for t in cursor]

            except mysql.connector.Error as error:
                print(error.msg)
                return None

            finally:
                cursor.close()

        else:
            raise ConnectionError('handler is disconnected')
            return None

    def explain_table(self, name, dictionary=True):
        '''return a list of dictionares, each one representing one field,
       with theirs parameters as keys'''
        def fix_parameters(field):
            type=field['type'].strip(')').split('(')
            field['type']=list(self.field_parameters['type'].keys())[list(self.field_parameters['type'].values()).index(type[0].upper())]
            
            field['size']=None
            if len(type)==2:
                field['size']=int(type[1])
            
            field['null'] = True if field['null']=='Yes' else False
                    
            field['primary']=False
            field['unique']=False
            if field['key']=='PRI':
                field['primary']=True
                field['unique']=True
            elif field['key']=='UNI':
                field['unique']=True
            del field['key']

            field['auto_increment']=True if 'auto_increment' in field['extra'] else False
            del field['extra']

        if self.isconnected():
            try:
                cursor=self._connection.cursor()
                statement='EXPLAIN `%s`;'
                print(statement%name)
                cursor.execute(statement%name)

                if dictionary:
                    fields={}
                    for field in cursor:
                        parameters={}
                        it=iter(cursor.description)
                        next(it)
                        for parameter in it:
                            parameters[parameter[0].lower()] = field[cursor.description.index(parameter)]
                        fields[field[0]]=parameters
                    
                    for field in fields.values():
                        fix_parameters(field)

                    return fields

                else:
                    fields=[]
                    for field in cursor:
                        parameters={}
                        for parameter in cursor.description:
                            parameters[parameter[0].lower()] = field[cursor.description.index(parameter)]
                        fields.append(parameters)

                    for field in fields:
                        fix_parameters(field)

                    return fields

            except mysql.connector.Error as error:
                print(error.msg)
                return None

            finally:
                cursor.close()
        else:
            raise ConnectionError('handler is disconnected')
            return None

    def create_table(self, name, id=True, *args, **kwargs):
        '''create empty new table with name, 
        if id=True then first field id INT PRIMARY_KEY, AUTO_IN'''
        if self.isconnected():
            try:
                cursor=self._connection.cursor()
                statement=''

                if id:
                    statement='CREATE TABLE `%s` (`id` INT(11) NOT NULL PRIMARY KEY AUTO_INCREMENT);'
                    cursor.execute(statement%name)
                else:
                    statement='CREATE TABLE `%s` (`BLANK_FIELD_TEMPORARY` BOOL);'
                    cursor.execute(statement%name)
                    self.add_field(name, *args, **kwargs)
                    self.del_field(name, 'BLANK_FIELD_TEMPORARY')

            except mysql.connector.Error as error:
                print(error.msg)
                self._connection.rollback()

            else:
                self._connection.commit()

            finally:
                cursor.close()
        else:
            raise ConnectionError('handler is disconnected')


    def del_table(self, table):
        '''drops table from database'''
        if self.isconnected():
            try:
                cursor=self._connection.cursor()
                statement='DROP TABLE `%s`'%table
                cursor.execute(statement)
                
            except mysql.connector.Error as error:
                print(error.msg)
                self._connection.rollback()

            else:
                self._connection.commit()

            finally:
                cursor.close()

        else:
            raise ConnectionError('handler is disconnected')


    def add_field(self, table, field, type, size=None, unique=False, primary=False, default=None, null=True, auto_increment=False):
        '''add fields to table, based on arguments
        null=False has to be always provided with default value
        '''
        if self.isconnected():
            try:
                cursor=self._connection.cursor()
                statement='ALTER TABLE `%s` ADD `%s` %s %s %s %s %s %s'
                data=[]

                data.append(table)
                data.append(field)

                if issubclass(type,datetime):
                    data.append(self.field_parameters['type'][type])
                else:
                    if size is None:
                        size=self.field_parameters['default_size'][type]
                    data.append('%s(%d)'%(self.field_parameters['type'][type],size))

                data.append(self.field_parameters['unique'][unique])
                data.append(self.field_parameters['primary'][primary])
                data.append(self.field_parameters['null'][null])
                data.append(self.field_parameters['auto_increment'][auto_increment])
                if (default is not None) and isinstance(default,type):
                    data.append(self.field_parameters['default'][True]%str(default))
                else:
                    data.append(self.field_parameters['default'][False])
                
                #print(statement%tuple(data))
                cursor.execute(statement%tuple(data))

            except (KeyError, TypeError):
                print(sys.exc_info()[0], sys.exc_info()[1])
                self._connection.rollback()
            except mysql.connector.Error as error:
                print(error.msg)
                self._connection.rollback()

            else:
                self._connection.commit()

            finally:
                cursor.close()
        else:
            raise ConnectionError('handler is disconnected')


    def _foreign_key_deletion_fix(self, table, field):
        try:
            cursor=self._connection.cursor()
            statement='ALTER TABLE `%s` DROP FOREIGN KEY `%s`;'
            constraint_statement='SELECT `CONSTRAINT_NAME` FROM `KEY_COLUMN_USAGE` WHERE `TABLE_NAME`=\'%s\' AND `COLUMN_NAME`=\'%s\';'
            
            cursor.execute('USE `information_schema`;')
            
            cursor.execute(constraint_statement%(table,field))
            constraint_name=next(cursor)[0]

            cursor.execute('USE `%s`;'%self._config['database'])
            cursor.execute(statement%(table,constraint_name))


        except mysql.connector.Error as error:
            print(error.msg)
            self._connection.rollback()
        else:
            self._connection.commit()
        finally:
            cursor.close()

    def del_field(self, table, field):
        '''drops field from table'''
        if self.isconnected():
            try:
                cursor=self._connection.cursor()
                statement='ALTER TABLE `%s` DROP `%s`'
                cursor.execute(statement%(table,field))

            except TypeError:
                print('Invalid parameter',sys.exc_info()[1])
                self._connection.rollback()
            except mysql.connector.Error as error:
                if error.errno == 1090:
                    self.del_table(table)
                elif error.errno == 1553 or error.errno == 1025:
                    self._foreign_key_deletion_fix(table,field)
                    self.del_field(table,field)
                else: 
                    print(error.msg,error.errno)
                    self._connection.rollback()

            else:
                self._connection.commit()

            finally:
                cursor.close()
        
        else:
            raise ConnectionError('handler is disconnected')

    def add_data(self, table, *args, **kwargs):
        '''insert rows into table'''
        if self.isconnected():
            try:
                cursor=self._connection.cursor()

                statement='INSERT INTO `%s` (%s) VALUES (%s);'

                arg=dict.fromkeys([field['field'] for field in self.explain_table(table,dictionary=False)])

                for k,v in kwargs.items():
                    if k in arg:
                        arg[k]=v

                for v in args:
                    for k in arg:
                        if arg[k] is None:
                            arg[k]=v
                            break

                fields=[]
                values=[]

                for k,v in arg.items():
                    if v is not None:
                        fields.append('`%s`'%k)
                        if type(v)==type(datetime.now()):
                            values.append('NOW()')
                        else:
                            values.append('\'%s\''%str(v))
                

                print(statement%(table,','.join(fields),','.join(values)))
                cursor.execute(statement%(table,','.join(fields),','.join(values)))
            
            except KeyError:
                print(sys.exc_info()[1])
                self._connection.rollback()

            except mysql.connector.Error as error:
                print(error.msg)
                self._connection.rollback()

            else:
                self._connection.commit()

            finally:
                cursor.close()

        else:
            raise ConnectionError('handler is disconnected')

    
    def del_rows(self, table, truncate=False, wildcard=True, logic='and', **conditions):
        '''del rows from the table meeting all the conditions
        truncate=True and empty conditions resets auto_increment
        '''
        if self.isconnected():
            try:
                cursor=self._connection.cursor()

                if len(conditions)==0 and truncate==True:
                    statement='TRUNCATE TABLE `%s`;'
                    cursor.execute(statement%table)
                else:
                    statement='DELETE FROM `%s` WHERE %s;'
                    condition=_prepare_conditions(logic, wildcard, **conditions)
                    cursor.execute(statement%(table,condition))

            except mysql.connector.Error as error:
                print(error.msg)
                self._connection.rollback()

            else:
                self._connection.commit()

            finally:
                cursor.close()

        else:
            raise ConnectionError('handler is disconnected')


    def list_rows(self, table, logic='and', wildcard=True, join=None, include_null=False, dictionary=True, order_by=None, **conditions):
        '''returns all the rows from the table meeting all the conditions
        name='=abc' , id='<5'   empty conditions = all rows
        ''' 
        def _join_relation_checks(table,join):
            try:
                cursor=self._connection.cursor(dictionary=True, buffered=True)
                cursor.execute('USE `information_schema`;')               
                statement='SELECT `COLUMN_NAME`,`REFERENCED_COLUMN_NAME` FROM `KEY_COLUMN_USAGE` WHERE `TABLE_NAME`=\'%s\' AND `REFERENCED_TABLE_NAME`=\'%s\''
                #print(statement%(table,join))
                cursor.execute(statement%(table,join))
            except mysql.connector.Error as error:
                print(error.msg)
                self._connection.rollback()
            else:
                ret=[(x['COLUMN_NAME'],x['REFERENCED_COLUMN_NAME']) for x in cursor]
                self._connection.commit()
                cursor.execute('USE `%s`'%self._config['database'])
                return ret
            finally:
                cursor.close()
                
        if self.isconnected():
            try:
                cursor=self._connection.cursor(dictionary=dictionary, buffered=True)

                statement='SELECT * FROM `%s` %s WHERE %s %s;'
                condition=_prepare_conditions(logic, wildcard, **conditions)

                join_data=[]
                if join is not None:
                    join_template='JOIN `%s` ON (`%s`.`%s`=`%s`.`%s`)'
                    if include_null:
                        join_template='LEFT '+join_template

                    if isinstance(join,tuple):
                        for j in join:
                            relations=_join_relation_checks(table,j)
                            for r in relations:
                                join_data.append(join_template%(j,table,r[0],j,r[1]))
                    elif isinstance(join,str):
                        relations=_join_relation_checks(table,join)
                        for r in relations:
                            join_data.append(join_template%(join,table,r[0],join,r[1]))
                
                order_stm=''
                if isinstance(order_by,dict) and len(order_by)>0:
                    order_stm='ORDER BY %s'
                    ordering_keynames=('ASC','DESC')
                    order=[]
                    template='`%s` %s'
                            
                    for k,v in order_by.items():
                        if v.upper() in ordering_keynames:
                            order.append(template%(k,v))

                    order_stm=order_stm%','.join(order)
                    
                print(statement%(table,' '.join(join_data),condition,order_stm))
                cursor.execute(statement%(table,' '.join(join_data),condition,order_stm))

            except mysql.connector.Error as error:
                print(error.msg)
                self._connection.rollback()
                return None

            else:
                self._connection.commit()
                ret=[row for row in cursor]
                if not dictionary:
                    desc=[field[0] for field in cursor.description]
                    return desc,ret
                else:
                    return ret

            finally:
                cursor.close()
    
        else:
            raise ConnectionError('handler is disconnected')
            return None
    
    def update_rows(self, table,logic='and', wildcard=True, **conditions):
        if self.isconnected():
            try:
                cursor=self._connection.cursor()
                statement='UPDATE `%s` SET %s WHERE %s;'
                updates,condition=_prepare_conditions(logic,wildcard,update=True,**conditions)
                assert updates is not None
                update_statement=','.join(updates)
                
                print(statement%(table,update_statement,condition))
                cursor.execute(statement%(table,update_statement,condition))
                
            except mysql.connector.Error as error:
                print(error.msg)
                self._connection.rollback()
            except AssertionError:
                print(sys.exc_info()[1])
                self._connection.rollback()
            
            else:
                self._connection.commit()
                
            finally:
                cursor.close()
        else:
            raise ConnectionError('handler is disconnected')
    
    def create_relation(self, table1, table2, field1=None, field2=None, unique=False, delete='NO ACTION', update='NO ACTION'):
        
        if self.isconnected():
            try:
                actions=('CASCADE','SET NULL','RESTRICT','NO ACTION','SET DEFAULT')
                statement='ALTER TABLE `%s` ADD `%s` %s %s,ADD FOREIGN KEY (`%s`) REFERENCES `%s`(`%s`) ON DELETE %s ON UPDATE %s;'

                cursor=self._connection.cursor()

                if field1 is None:
                    field1='%s_id'%table2
                if field2 is None:
                    field2='id'

                type=self.field_parameters['type'][self.explain_table(table2)[field2]['type']]
                
                data=[table1,field1,type]
                data.append(self.field_parameters['unique'][unique])
                data.extend([field1,table2,field2])
                if delete in actions and update in actions:
                    data.extend([delete,update])
                else:
                    raise ValueError('delete or update arguments not good')

                print(statement%tuple(data))
                cursor.execute(statement%tuple(data))

            except mysql.connector.Error as error:
                print(error.msg)
                self._connection.rollback()

            except (KeyError, ValueError, TypeError):
                print(sys.exc_info()[1],sys.exc_info()[2])
                self._connection.rollback()

            else:
                self._connection.commit()

            finally:
                cursor.close()
        else:
            raise ConnectionError('handler is disconnected')
