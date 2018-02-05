import csv
import numpy as np
import pandas as pd
import sqlite3
import os
from datetime import datetime, date
from math import sin, cos, sqrt, atan2, radians

class student():
    def __init__(self,stuID,studentsMeta, classSchedule):
        # find studnet data whose ID is stuID in Student Meta data
        stuMeta = [i for i in studentsMeta if i[0] == stuID][0]
        self.files = []
        self.stupath = '/Users/gimduyeon/Dropbox (KAIST Dr.M)/KAIST Dr.M의 팀 폴더/공유/rawdata/'
        self.time_format = '%Y.%m.%d_%H.%M.%S'  # timestamp format
        self.numToDay = {1: 'Mo', 2: 'Tu', 3: 'We', 4: 'Th', 5: 'Fr', 6: 'Sa', 7: 'Su'}
        # set meta data about this studnet
        self.stuID = stuMeta[0]
        #self.sex = stuMeta[1]
        #self.highschool = stuMeta[2]
        #self.total = stuMeta[3]
        #self.enrolled = stuMeta[4]
        #self.drop = stuMeta[5]
        self.classes = stuMeta[6].split('~')
        self.classesSchedule = classSchedule  # all classes schedule

        # set default wifi log, classes information that student enrolled,
        # wifi logs in classtime, and attendance
        self.StuClassInfo = []
        self.wifiLogsInClass = {i: [] for i in self.classes}
        self.gpsLogsInClass = {i: [] for i in self.classes}
        #self.scannedAP = {}
        self.attendanceByWiFi = {}
        self.attendanceByGPS = {}


    def getlog(self,stuID):
        files = os.listdir(self.stupath + str(stuID))
        self.files = sorted(files)
        for i in self.files:
            timestamp = datetime.strptime(i.split("g_")[1].split(".db")[0], '%Y.%m.%d_%H.%M.%S')
            weekday = self.numToDay[date.isoweekday(timestamp)]
            daytime = timestamp.time()

            ClassStart = datetime.strptime('8.0.0','%H.%M.%S').time()
            ClassEnd = datetime.strptime('20.0.0','%H.%M.%S').time()
            if weekday != 'Sa' and weekday != 'Su' and ClassStart < daytime < ClassEnd:
                wifiLog = []
                conn = sqlite3.connect(self.stupath + str(stuID) + '/' + i)
                cur = conn.cursor()
                cur.execute('SELECT * FROM HARDWARETABLE')
                [wifiLog.append(row) for row in cur.fetchall() if row[2] == 'WIFI1' or row[2] == 'WIFI2']
                for j in wifiLog:
                    for k in range(len(self.classes)):
                        classtime1 = self.StuClassInfo.iloc[k, 4]  # 수업 시간
                        classtime2 = self.StuClassInfo.iloc[k, 5]  # 수업 시간

                        if weekday == classtime1.split("~")[0].split(".")[0]:
                            classtime = [datetime.strptime(_.split(".")[1], '%H:%M') for _ in classtime1.split("~")]
                            if classtime[0].time().minute < 15:
                                classtime[0] = classtime[0].replace(hour=classtime[0].time().hour - 1,
                                                                    minute=classtime[0].time().minute - 15 + 60)
                                if classtime[0].time()<daytime<classtime[1].time():
                                    self.wifiLogsInClass[self.classes[k]].append(j)
                            else:
                                classtime[0] = classtime[0].replace(minute=classtime[0].time().minute - 15)
                                if classtime[0].time() <daytime < classtime[1].time():
                                    self.wifiLogsInClass[self.classes[k]].append(j)
                        if classtime2 != '' and weekday == classtime2.split("~")[0].split(".")[0]:
                            classtime = [datetime.strptime(_.split(".")[1], '%H:%M') for _ in classt2.split("~")]
                            if classtime[0].time().minute < 15:
                                classtime[0] = classtime[0].replace(hour=classtime[0].time().hour - 1,
                                                                    minute=classtime[0].time().minute - 15 + 60)
                                if classtime[0].time() < daytime < classtime[1].time():
                                    self.wifiLogsInClass[self.classes[k]].append(j)

                            else:
                                classtime[0] = classtime[0].replace(minute=classtime[0].time().minute - 15)
                                if classtime[0].time() < daytime < classtime[1].time():
                                    self.wifiLogsInClass[self.classes[k]].append(j)

                gpsLog = []
                cur = conn.cursor()
                cur.execute('SELECT * FROM GPSTABLE')
                [self.gpsLog.append(row) for row in cur.fetchall()]

                    conn.close()


    def getStuClassInfo(self,classSchedule):
        stuclass = pd.DataFrame(columns=['class', 'major', 'part', 'loc', 't1', 't2', 'BSSID', 'gpscoor'])
        for c in range(len(self.classes)):
            stuclass = pd.concat([stuclass, classSchedule[classSchedule['class'] == self.classes[c]]],
                                 join='outer')

        self.StuClassInfo = stuclass


    def checkAttendanceByWiFi(self):

    def checkDistanceFromBuilding(self):

    def checkAttendanceByGPS(self):



if __name__=='__main__':
    path = '/Users/gimduyeon/Google Drive File Stream/My Drive/gitlab/drm-project/data'

    #import files: AP list file, classrooms' gps coordinate file
    apFile = open(path+'/classroomap_drm.csv','r')
    gpsFile = open(path+'/buildingGPS.csv','r')

    aplist = [r for r in csv.reader(apFile)]
    aplist = [[i[0], i[1][1:-1].replace('"', '').split(",")] for i in aplist]

    gpslist = [r for r in csv.reader(gpsFile)]
    gpslist = [[i[0], float(i[1]), float(i[2]), int(i[3])] for i in gpslist]

    # get the classes' data
    conn = sqlite3.connect(path + '/SCHEDULEDATABASE.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM SCHEDULETABLE')
    classesData = [list(row) for row in cur.fetchall()]

    # get the students' data
    studentsMeta = []
    conn = sqlite3.connect(path + '/METADATABASE.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM METATABLE')
    rows = cur.fetchall()
    for row in rows:
        studentsMeta.append(row)

    #add ap data to classes data
    for i in classesData:
        [i.append(j[1]) for j in aplist if j[0] == i[3]]
        [i.append(k) for k in gpslist if k[0] == i[3].split('(')[1].split(')')[0]]
    classesData_pd = pd.DataFrame(classesData,
                                    columns=['class', 'major', 'part', 'loc', 't1', 't2', 'BSSID', 'gpscoor'])



    # sample students list
    testStu = [24897713, 25063234, 29962358, 32646509, 40523189, 42236646, 43261317,
               53789312, 57623951, 58201289, 63987688, 87155919, 93775784, 95983626]


    students = {i: 0 for i in testStu}

    for i in testStu:
        students[i] = student(i, studentsMeta, classesData_pd)
        students[i].studentUpdate()
        writer = pd.ExcelWriter(path+'/attendanceResultswithGPS/'+str(students[i].stuID)+'.xlsx')
        workbook = writer.book
        for j in students[i].attendance.columns:
            eachclass = pd.DataFrame(students[i].attendance.ix[students[i].attendance[j].values != '', j])
            eachclass.to_excel(writer, j[:25])
        writer.save()