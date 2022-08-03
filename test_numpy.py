import mysql.connector
import datetime

mydb = mysql.connector.connect(
  host="120.77.86.169",       # 数据库主机地址
  user="reader",    # 数据库用户名
  passwd="GLspepc2019*",   # 数据库密码
  database= "landslide_early_warning"
)

mycursor =mydb.cursor()  # 使用 cursor() 方法创建一个游标对象
# sql = "INSERT INTO site (name,url) values (%s,%s)"
#
# val=[('Github', 'https://www.github.com'),
#   ('Taobao', 'https://www.taobao.com')
# ]
# mycursor.executemany(sql,val)
# mydb.commit()
sql1 ="""
SET @selectDate = unix_timestamp('2019-11-27')"""


sql2="""SET @beginTime = @selectDate - 3 * 3600 """

sql3="""
SELECT
	m.deviceId AS '网关',
	p.t_monitorPoint_location AS '地址',
	p.t_monitorPoint_person AS '负责人',
	p.t_monitorPoint_remark AS '电话',
	log.deviceId AS '采集点',
	log.add_time AS '时间',
	CASE
WHEN unix_timestamp(log.add_time) > @beginTime
AND unix_timestamp(log.add_time) < @beginTime + 6 * 3600 THEN
	'0'
WHEN unix_timestamp(log.add_time) > @beginTime + 6 * 3600
AND unix_timestamp(log.add_time) < @beginTime + 12 * 3600 THEN
	'6'
WHEN unix_timestamp(log.add_time) > @beginTime + 12 * 3600
AND unix_timestamp(log.add_time) < @beginTime + 18 * 3600 THEN
	'12'
WHEN unix_timestamp(log.add_time) > @beginTime + 18 * 3600
AND unix_timestamp(log.add_time) < @beginTime + 24 * 3600 THEN
	'18'
ELSE
	'unknown'
END AS `date`,
 CONCAT(
	substring_index(
		substring_index(log.body, ':' ,- 1),
		'}',
		1
	) / 10,
	'°'
) AS '角度'
FROM
	t_protocol_log AS log
LEFT JOIN t_detectionincline AS d ON log.deviceId = d.deviceId
LEFT JOIN t_mastercontrol AS m ON d.controlSn = m.deviceSn
LEFT JOIN t_monitorpoint_mastercontrol AS pm ON pm.masterControl_id = m.id
LEFT JOIN t_monitor_point AS p ON pm.monitorPoint_id = p.id
WHERE
	unix_timestamp(log.add_time) > @beginTime
AND unix_timestamp(log.add_time) < @beginTime + 24 * 3600
AND log.msgCode = 30
AND log.deviceType = 101
AND m.deviceId > '999'
AND m.deviceId < '2000'
GROUP BY
	log.deviceId,
	`date`
ORDER BY
	m.deviceId ASC,
	log.deviceId ASC,
	log.add_time ASC
"""
mycursor.execute(sql1)
mycursor.execute(sql2)
mycursor.execute(sql3)
my=mycursor.fetchall()
for i in my :
    print(i)


