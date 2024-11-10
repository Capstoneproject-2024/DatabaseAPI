import pymysql
from dotenv import load_dotenv
import os
from flask import Flask, jsonify, request
import csv

app = Flask(__name__)

load_dotenv()

class DBManager:
    def __init__(self):
        # 데이터베이스 연결
        self.conn = pymysql.connect(
            host='127.0.0.1',
            user=os.environ.get("user"),
            password=os.environ.get('pw'),
            db=os.environ.get("db"),
            charset='utf8'
        )
        self.cur = self.conn.cursor()

        # userTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS userTable (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                nickname VARCHAR(45) NOT NULL,
                email VARCHAR(254) NOT NULL,
                uid VARCHAR(45) NOT NULL
            );
        """)

        # bookTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS bookTable (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                author VARCHAR(100),
                publisher VARCHAR(30),
                year VARCHAR(4),
                image TEXT,
                ISBN CHAR(13)
            );
        """)

        # followerTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS followerTable (
                userID1 INT NOT NULL,
                userID2 INT NOT NULL,
                PRIMARY KEY (userID1, userID2),
                FOREIGN KEY (userID1) REFERENCES userTable(ID) ON DELETE CASCADE,
                FOREIGN KEY (userID2) REFERENCES userTable(ID) ON DELETE CASCADE
            );
        """)

        # followRequestTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS followRequestTable (
                senderID INT NOT NULL,
                receiverID INT NOT NULL,
                PRIMARY KEY (senderID, receiverID),
                FOREIGN KEY (senderID) REFERENCES userTable(ID) ON DELETE CASCADE,
                FOREIGN KEY (receiverID) REFERENCES userTable(ID) ON DELETE CASCADE
            );
        """)

        # bookKeywordTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS bookKeywordTable (
                bookID INT NOT NULL PRIMARY KEY,
                keyword TEXT
            );
        """)

        # bookReviewKeywordTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS bookReviewKeywordTable (
                bookID INT NOT NULL PRIMARY KEY,
                reviewKeyword TEXT
            );
        """)

        # groupTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS groupTable (
                groupID INT AUTO_INCREMENT PRIMARY KEY,
                groupName VARCHAR(100) NOT NULL,
                adminID INT NOT NULL
            );
        """)

        # groupMemberTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS groupMemberTable (
                groupID INT,
                memberID INT NOT NULL,
                PRIMARY KEY (groupID, memberID),
                FOREIGN KEY (groupID) REFERENCES groupTable(groupID) ON DELETE CASCADE,
                FOREIGN KEY (memberID) REFERENCES userTable(ID) ON DELETE CASCADE
            );
        """)

        # reviewTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS reviewTable (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                userID INT NOT NULL,
                bookID INT NOT NULL,
                rating FLOAT CHECK (rating >= 0 AND rating <= 5),
                review TEXT,
                reviewDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (userID) REFERENCES userTable(ID) ON DELETE CASCADE,
                FOREIGN KEY (bookID) REFERENCES bookTable(id) ON DELETE CASCADE
            );
        """)

        # reviewCommentTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS reviewCommentTable (
                commentID INT AUTO_INCREMENT PRIMARY KEY,
                reviewID INT NOT NULL,
                userID INT NOT NULL,
                comment TEXT NOT NULL,
                commentDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (reviewID) REFERENCES reviewTable(ID) ON DELETE CASCADE,
                FOREIGN KEY (userID) REFERENCES userTable(ID) ON DELETE CASCADE
            );
        """)
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS reviewVisibilityTable (
                reviewID INT NOT NULL,
                groupID INT NOT NULL,
                visibilityLevel ENUM('public', 'group', 'private') DEFAULT 'public',
                PRIMARY KEY (reviewID, groupID),
                FOREIGN KEY (reviewID) REFERENCES reviewTable(ID) ON DELETE CASCADE,
                FOREIGN KEY (groupID) REFERENCES groupTable(groupID) ON DELETE CASCADE
            );

        """)


        # 책 데이터가 없다면 CSV 파일에서 데이터 삽입
        self.cur.execute("SELECT COUNT(*) FROM bookTable")
        result = self.cur.fetchone()

        if result[0] == 0:
            with open('finalData.csv', mode='r') as file:
                csv_reader = csv.reader(file)

                for row in csv_reader:
                    if len(row) == 6:  # 데이터가 6개 컬럼과 맞는지 확인
                        name = row[0][:50]
                        author = row[1][:100]
                        publisher = row[2][:30]
                        year = row[3][:4]
                        image = row[4][:255]
                        isbn = row[5][:13]

                        query = """
                            INSERT INTO bookTable (name, author, publisher, year, image, ISBN)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        self.cur.execute(query, (name, author, publisher, year, image, isbn))
                    else:
                        print(f"Skipping invalid row: {row}")

            # 변경 사항 커밋
            self.conn.commit()

    def execute_query(self, query):
        try:
            # SQL 문 실행
            self.cur.execute(query)
            
            # SELECT 문인 경우 결과 가져오기
            if query.strip().lower().startswith("select"):
                result = self.cur.fetchall()
                return result
            else:
                # SELECT 문이 아닌 경우 (예: INSERT, UPDATE, DELETE)
                self.conn.commit()
                print("쿼리 실행 완료.")
        except Exception as e:
            print(f"에러 발생: {e}")

    def close(self):
        self.conn.close()


# DBManager 인스턴스 생성 및 실행
dbManager = DBManager()

@app.route('/')
def main():
    return 'main'

@app.route('/execute_query', methods=['GET'])
def execute_query_route():
    query = request.args.get('query')  # URL의 query 파라미터를 가져옴
    if query:
        result = dbManager.execute_query(query)
        return jsonify({"result": result})  # 결과를 JSON 형식으로 반환
    else:
        return jsonify({"error": "쿼리를 입력해주세요."}), 400

if __name__ == '__main__':
    app.run('0.0.0.0', port=5001, debug=True)
