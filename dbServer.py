from clickhouse_driver.dbapi import connect

class DBServer:
    def __init__(self,host,port,user,password,database):
        self.conn = connect(host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                compression=True)
        self.cursor = self.conn.cursor()
        
    def fetchall(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()
        
        
    
    def fetchmany(self,sql,size):
        self.cursor.execute(sql)
        return self.cursor.fetchmany(size)    
        
    
    def fetchone(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchone()
        
    def execute(self,sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            self.conn.rollback()

    
    def update(self,sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            self.conn.rollback()
            
            
    def delete(self,sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            self.conn.rollback()
    
    def close(self):
        self.cursor.close()
        self.conn.close()



