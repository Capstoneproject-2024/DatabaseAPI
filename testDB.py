import pymysql
from dotenv import load_dotenv
import os

load_dotenv()


class DBManager:
    def __init__(self):
        self.conn = pymysql.connect(host = '127.0.0.1',user = os.environ.get("user"),password = os.environ.get('pw'), db = os.environ.get("db"), charset = 'utf8')
        self.cur = self.conn.cursor()
        self.cur.execute("CREATE TABLE userTable (id char(4), userName char(15), email char(20), birthYear int)" )
        self.cur.execute("INSERT INTO userTable VALUES('hong', '홍지윤', 'hong@naver.com', 1996)")
        self.cur.execute("INSERT INTO userTable VALUES('kim', '김태연', 'kim@daum.com', 2011)")
        self.cur.execute("INSERT INTO userTable VALUES('star', '별사랑', 'star@paran.com', 1990)")
        self.cur.execute("INSERT INTO userTable VALUES('yang', '양지은', 'yang@gmail.com', 1993)")
        self.conn.commit()
        self.conn.close()


m= DBManager()
