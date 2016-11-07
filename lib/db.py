#!/usr/bin/python

#Description:
#---------------------
#MySQL manipulation
#---------------------
#Create by coder-james

import MySQLdb
import conf

class mysql:
  def __init__(self):
    self.db = MySQLdb.connect(conf.mysql_ipaddr, conf.mysql_user, conf.mysql_pwd, conf.mysql_db)

  def fetchall(self, sql):
    print "sql statement: %s" %sql
    try:
      cursor = self.db.cursor()
      cursor.execute(sql)
      return cursor.fetchall()
    except Exception,e:
      print e

  def select(self, table, target="*", wheres=[], orderindex=None, **kwargs):
    sql = "select %s from %s " %(target, table)
    if len(wheres) != 0:
      sql += "where %s " %(" and ".join(wheres))
    for key,value in kwargs.items():
      if key == "LIMIT":
        sql += "LIMIT %s " % value
    if orderindex != None:
      sql += "order by %s" % orderindex
    sql += ";"
    return self.fetchall(sql)

  def selectmulti(self, tables=[], target="*", key="", wheres=[], orderindex=None, **kwargs):
    if len(tables) == 0:
      return None
    elif len(tables) == 1:
      print "please use `select` instead"
      return None
    else:
      sql = "select %s from %s " % (target, ",".join(tables))
      tableNum = len(tables)
      sql += "where "
      for j in range(tableNum - 1):
        sql += "%s.%s = %s.%s and " %(tables[j],key,tables[j+1],key)
      if len(wheres) != 0:
        sql += "%s " %(" and ".join(wheres))
      else:
        sql = sql[:-4]
      for key,value in kwargs.items():
        if key == "LIMIT":
          sql += "LIMIT %s " % value
      if orderindex != None:
        sql += "order by %s" % orderindex
      sql += ";"
      return self.fetchall(sql)

  def close(self):
    self.db.close()

if __name__ == "__main__":
  db = mysql()
  #print db.fetchall("select * from spark_task_metrics_summary LIMIT 1;")
  print db.select("spark_task_metrics_summary", LIMIT="1")
  db.close()
