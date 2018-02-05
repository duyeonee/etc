####################################################
# wifi 출석 검증 이슈 - 20180129
# To do list
# 데이터 포멧에 맞게 출력하여 주기
# wifi 버전과 wifi&gps를 둘다 사용하여 낸 결과값 두개 출력
####################################################
import csv
import numpy as np
import pandas as pd
import sqlite3
import os
from datetime import datetime, date
from math import sin, cos, sqrt, atan2, radians


# class of student
class student():
    def __init__(self, stuID, studentsMeta, classsSchedule):
        # find studnet data whose ID is stuID in Student Meta data
        stuMeta = [i for i in studentsMeta if i[0] == stuID][0]
        self.files = []
        self.stupath = '/Users/gimduyeon/Dropbox (KAIST Dr.M)/KAIST Dr.M의 팀 폴더/공유/rawdata/'
        # set meta data about this studnet
        self.stuID = stuMeta[0]
        self.sex = stuMeta[1]
        self.highschool = stuMeta[2]
        self.total = stuMeta[3]
        self.enrolled = stuMeta[4]
        self.drop = stuMeta[5]
        self.classes = stuMeta[6].split('~')
        self.classesSchedule = classsSchedule  # all classes schedule

        # set default wifi log, classes information that student enrolled,
        # wifi logs in classtime, and attendance
        self.log = []
        self.StuClassInfo = []
        self.logsInClass = {}
        self.scannedAP = {}
        self.attendance = {}

    # function that gets the student total wifi scanning log
    def getStuLog(self, stuID):
        files = os.listdir(self.stupath + str(stuID))
        self.files = sorted(files)
        logs = []
        for i in self.files:
            conn = sqlite3.connect(self.stupath + str(stuID) + '/' + i)
            cur = conn.cursor()
            cur.execute('SELECT * FROM HARDWARETABLE')
            [logs.append(row) for row in cur.fetchall() if row[2] == 'WIFI1' or row[2] == 'WIFI2']
        self.log = logs


    # function that gets classes information that student enrolled
    def getStuClassInfo(self, classSchedule):
        stuclass = pd.DataFrame(columns=['class', 'major', 'part', 'loc', 't1', 't2', 'BSSID'])
        for c in range(len(self.classes)):
            stuclass = pd.concat([stuclass, classSchedule[classSchedule['class'] == self.classes[c]]],
                                 join='outer')

        self.StuClassInfo = stuclass

    # function that gets the wifi scanning logs, that timestamps are in class time
    def getLogsInClass(self):
        logsinClass = {i: [] for i in self.classes}
        gpslogsinClass = {i: [] for i in self.classes}
        data_log_format = '%Y.%m.%d_%H.%M.%S'  # timestamp format

        # date 라이브러리에서 날짜가 월요일 ~ 일요일 까지 1~7로 매핑되어있다.
        # 숫자로 매핑된 날짜정보를 문자로 수정하기 위한 num_to_day
        num_to_day = {1: 'Mo', 2: 'Tu', 3: 'We', 4: 'Th', 5: 'Fr', 6: 'Sa', 7: 'Su'}

        # 수업 시간 도중에 발생한 wifi scanning data filtering 방법
        # 1. 전체 wifi scanning log를 하나씩 확인하다.
        # 2. 해당 log가 학생이 수강 중인 어떤 수업의 날짜와 같고, 시간이 그 수업 시간에 포함될때 -> 해당 수업 중 발생한 wifi scanning log
        # 그 이외의 경우 해당 수업시간에 발생한 wifi scanning log로 판별하지 않음
        for i in self.log:
            timestamp = datetime.strptime(i[1], data_log_format)  # logs' string format convert to datetime format
            weekday = num_to_day[date.isoweekday(timestamp)]  # 로그가 무슨 요일인지 확인
            daytime = timestamp.time()  # 로그가 몇시 몇분인지 확인

            # 전체 수업 중 로그가 어떤 수업 중 발생한 것인지 판별
            for j in range(len(self.classes)):
                classt1 = self.StuClassInfo.iloc[j, 4]  # 수업 시간
                if weekday == classt1.split("~")[0].split(".")[0]:  # 로그가 해당 수업이 있는 요일에 발생한 경우
                    classtime = [datetime.strptime(_.split(".")[1], '%H:%M') for _ in classt1.split("~")]
                    if classtime[0].time().minute < 15:
                        classtime[0] = classtime[0].replace(hour=classtime[0].time().hour - 1,
                                                            minute=classtime[0].time().minute - 20 + 60)
                        if daytime > classtime[0].time() and daytime < classtime[
                            1].time():  # 로그가 해당 수업 시간 중 발생하였을 때 로그를 해당 수업에 저장
                            logsinClass[self.classes[j]].append(i)
                    else:
                        classtime[0] = classtime[0].replace(minute=classtime[0].time().minute - 20)
                        if daytime > classtime[0].time() and daytime < classtime[
                            1].time():  # 로그가 해당 수업 시간 중 발생하였을 때 로그를 해당 수업에 저장
                            logsinClass[self.classes[j]].append(i)

                classt2 = self.StuClassInfo.iloc[j, 5]
                if classt2 is not '':  # 수업이 일주일에 두번일 경우. 위와 같은 과정을 거침
                    if weekday == classt2.split("~")[0].split(".")[0]:
                        classtime = [datetime.strptime(_.split(".")[1], '%H:%M') for _ in classt2.split("~")]
                        if classtime[0].time().minute < 15:
                            classtime[0] = classtime[0].replace(hour=classtime[0].time().hour - 1,
                                                                minute=classtime[0].time().minute - 20 + 60)
                            if daytime > classtime[0].time() and daytime < classtime[
                                1].time():  # 로그가 해당 수업 시간 중 발생하였을 때 로그를 해당 수업에 저장
                                logsinClass[self.classes[j]].append(i)
                        else:
                            classtime[0] = classtime[0].replace(minute=classtime[0].time().minute - 20)
                            if daytime > classtime[0].time() and daytime < classtime[
                                1].time():  # 로그가 해당 수업 시간 중 발생하였을 때 로그를 해당 수업에 저장
                                logsinClass[self.classes[j]].append(i)


        self.logsInClass = logsinClass

    # function that checks attendance
    # 해당 수업 때 잡힌 ap의 갯수를 카운트 해서 리턴한다.
    def checkattend(self):
        print(self.stuID)
        # 2학기 날짜를 불러온다.
        path = '/Users/gimduyeon/Google Drive File Stream/My Drive/gitlab/drm-project/data'
        f = open(path + '/semesterday.csv', 'r')
        semesterday = [r[0] for r in csv.reader(f)]

        logAP = {i: {} for i in self.classes}
        for c in self.classes:
            # 수업 중 발생한 로그 중, unknown ssid는 제거하고 나머지 로그의 날짜, bssid, ssid를 가져온다
            logs = [[i[1].split("_")[0], i[4], i[5]] for i in self.logsInClass[c] if i[3] != '<unknown ssid>']
            logs_perday = {i[0]: [] for i in logs}  # 요일별로 데이터 묶기
            [logs_perday[j[0]].append([j[1], j[2]]) for j in logs]
            logAP[c] = logs_perday

        info = {i: self.StuClassInfo[self.StuClassInfo['class'] == i].BSSID.values[0] for i in
                self.classes}  # 수업 별 ap 리스트 정보
        countAP = {i: {j: "" for j in semesterday} for i in self.classes}
        listAP = {i: {j: "" for j in semesterday} for i in self.classes}

        # 데이터가 수집되기 전의 날짜를 처리
        for i in self.classes:
            for j in countAP[i]:
                if datetime.strptime(j, '%Y.%m.%d') < datetime.strptime(self.files[0].split("_")[1], '%Y.%m.%d'):
                    countAP[i][j] = "데이터 수집 전"
                    listAP[i][j] = "데이터 수집 전"

        for c in self.classes:
            for k in logAP[c]:
                if info[c] != None:  # 강의실 ap 정보가 없는 경우는 예외처리
                    # listAP[c][k] = set([m for m in logAP[c][k][0] if isinstance(m,str) and m[:-2] in [z[:-2] for z in info[c]]]) #tanimoto

                    listAP[c][k] = set([i[0] for i in logAP[c][k] if
                                        isinstance(i[0], str) and i[0] != None and i[0][:-2] in [z[:-2] for z in info[c]]])
                    # listAP[c][k] = set([m[0] for m in logAP[c][k] if m[0] in info[c]])

                    if len(listAP[c][k]) != 0: #wifi 핑거프린트가 tanimoto로 존재할 경우
                        countAP[c][k] = "출석"
                    else:
                        countAP[c][k] = "검출없음" #일치하는 ap가 없을 경우
                else:
                    print("강의실 ap정보 없음 ")
                    print(info[c])
        self.logAP = logAP
        self.info = info
        self.listAP = listAP
        listAP = pd.DataFrame(listAP)
        countAP = pd.DataFrame(countAP)

        num_to_day = {1: 'Mo', 2: 'Tu', 3: 'We', 4: 'Th', 5: 'Fr', 6: 'Sa', 7: 'Su'}
        for c in self.classes:
            classday = [i.split(".")[0] for i in self.StuClassInfo.ix[self.StuClassInfo['class'] == c, 4:6].values[0]]
            for t in countAP.loc[:, c].keys():
                if num_to_day[datetime.strptime(t, '%Y.%m.%d').isoweekday()] in classday and \
                                countAP[c][t] != '데이터 수집 전' and countAP[c][t] == "":
                    countAP.loc[t, c] = '검출없음'

        self.scannedAP = listAP
        self.attendance = countAP

    # function that executes other class methods
    def studentUpdate(self):
        self.getStuLog(self.stuID)
        self.getStuClassInfo(self.classesSchedule)
        self.getLogsInClass()
        self.checkattend()


##############################################
########   데이터 불러오기 및 출석 체크  ############
##############################################

if __name__ == '__main__':
    # open AP data csv file
    # path = '/Users/duyeonee/Documents/local/drm-project/data'
    path = '/Users/gimduyeon/Google Drive File Stream/My Drive/gitlab/drm-project/data'
    f = open(path + '/classroomap_drm.csv', 'r')
    # AP data preprocessing
    aplist = [r for r in csv.reader(f)]
    aplist = [[i[0], i[1][1:-1].replace('"', '').split(",")] for i in aplist]

    # get the classes' schedule data from scheduledatebase db file
    conn = sqlite3.connect(path + '/SCHEDULEDATABASE.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM SCHEDULETABLE')
    classschedule = [list(row) for row in cur.fetchall()]

    # integrate AP data and class schedule data by pandas format
    # integrate gps data and class schedule data by pandas format
    for i in classschedule:
        [i.append(j[1]) for j in aplist if j[0] == i[3]]
    classschedule_pd = pd.DataFrame(classschedule,
                                    columns=['class', 'major', 'part', 'loc', 't1', 't2', 'BSSID'])

    # get the student metadata from metadatabase db file
    studentsmeta = []
    conn = sqlite3.connect(path + '/METADATABASE.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM METATABLE')
    rows = cur.fetchall()
    for row in rows:
        studentsmeta.append(row)

    # 출석 체크 실행
    # sample students list

    testStu = [24897713, 25063234, 29962358, 32646509, 40523189, 42236646, 43261317,
               53789312, 57623951, 58201289, 63987688, 87155919, 93775784, 95983626]

    students = {i: 0 for i in testStu}

    for i in testStu:
        students[i] = student(i, studentsmeta, classschedule_pd)
        students[i].studentUpdate()
        writer = pd.ExcelWriter(path+'/attendanceResults/'+str(students[i].stuID)+'.xlsx')
        workbook = writer.book
        for j in students[i].attendance.columns:
            eachclass = pd.DataFrame(students[i].attendance.ix[students[i].attendance[j].values != '', j])
            eachclass.to_excel(writer, j[:25])


    writer.save()