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
    cursor = self.db.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

  def close(self):
    self.db.close()

if __name__ == "__main__":
  db = mysql()
  print db.fetchall("select * from spark_task_metrics_summary LIMIT 1;")
  db.close()
