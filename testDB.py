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
                nickname VARCHAR(45) NOT NULL ,
                email VARCHAR(254) NOT NULL UNIQUE,
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
                description TEXT,
                image TEXT,
                ISBN CHAR(13)
            );
        """)

        # followerTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS followerTable (
                followerID INT NOT NULL,
                followeeID INT NOT NULL,
                PRIMARY KEY (followerID, followeeID),
                FOREIGN KEY (followerID) REFERENCES userTable(ID) ON DELETE CASCADE,
                FOREIGN KEY (followeeID) REFERENCES userTable(ID) ON DELETE CASCADE
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
                keyword TEXT NOT NULL,
                FOREIGN KEY (bookID) REFERENCES bookTable(ID) ON DELETE CASCADE
                         
            );
        """)

        # bookReviewKeywordTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS bookReviewKeywordTable (
                bookID INT NOT NULL PRIMARY KEY,
                reviewKeyword TEXT NOT NULL,
                FOREIGN KEY (bookID) REFERENCES bookTable(ID) ON DELETE CASCADE
            );
        """)

        # groupTable 생성
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS groupTable (
                groupID INT AUTO_INCREMENT PRIMARY KEY,
                groupName VARCHAR(100) NOT NULL,
                groupDescription Text,
                adminID INT NOT NULL,
                FOREIGN KEY (adminID) REFERENCES userTable(ID) ON DELETE CASCADE
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
                review TEXT NOT NULL,
                quote TEXT NOT NULL,
                reviewDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (userID) REFERENCES userTable(ID) ON DELETE CASCADE,
                FOREIGN KEY (bookID) REFERENCES bookTable(ID) ON DELETE CASCADE
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
                reviewID INT NOT NULL PRIMARY KEY,
                
                visibilityLevel ENUM('public', 'private') DEFAULT 'public',
                
                FOREIGN KEY (reviewID) REFERENCES reviewTable(ID) ON DELETE CASCADE
            );

        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS groupVocabularyTable (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                
                vocabulary   varchar(20) not null
                
            );

        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS groupQuestionCandidateTable (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                groupID INT NOT NULL,
                vocabularyID INT,
                question TEXT NOT NULL,
                FOREIGN KEY (groupID) REFERENCES groupTable(groupID) ON DELETE CASCADE,
                FOREIGN KEY (vocabularyID) REFERENCES groupVocabularyTable(ID) ON DELETE CASCADE
            );

        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS groupQuestionTable (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                groupID INT NOT NULL,
                vocabularyID INT,
                question TEXT NOT NULL,
                FOREIGN KEY (groupID) REFERENCES groupTable(groupID) ON DELETE CASCADE,
                FOREIGN KEY (vocabularyID) REFERENCES groupVocabularyTable(ID) ON DELETE CASCADE
            );

        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS  groupQuestionQuotationTable (
                questionID INT,
                userID INT,
                bookID INT,
                quotation TEXT,
                PRIMARY KEY (questionID, userID),
                FOREIGN KEY (questionID) REFERENCES groupQuestionTable(ID) ON DELETE CASCADE,
                FOREIGN KEY (userID) REFERENCES userTable(ID) ON DELETE CASCADE,
                FOREIGN KEY (bookID) REFERENCES bookTable(ID) ON DELETE CASCADE
            );

        """)
        
        
        


        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS  questionRecommendBookTable (
                questionID INT,
                userID INT,
                bookID INT,
                PRIMARY KEY (questionID, userID, bookID),
                FOREIGN KEY (questionID) REFERENCES groupQuestionTable(ID),
                FOREIGN KEY (userID) REFERENCES userTable(ID),
                FOREIGN KEY (bookID) REFERENCES bookTable(ID)
            );


        """)

        self.cur.execute("""
            CREATE TABLE  IF NOT EXISTS  reviewRecommendBookTable (
                reviewBookID  INT,
                userID INT,
                recommendBookID  INT,
                PRIMARY KEY (reviewBookID, userID, recommendBookID),
                FOREIGN KEY (reviewBookID) REFERENCES bookTable(ID),
                FOREIGN KEY (userID) REFERENCES userTable(ID),
                FOREIGN KEY (recommendBookID) REFERENCES bookTable(ID)
            );


        """)
        


        # 책 데이터가 없다면 CSV 파일에서 데이터 삽입
        self.cur.execute("SELECT COUNT(*) FROM groupVocabularyTable")
        result = self.cur.fetchone()

        if result[0] == 0:
            with open('Nouns_List.csv', mode='r',encoding = "utf-8") as file:
                csv_reader = csv.reader(file)

                for  row in csv_reader:
                    if len(row) == 1:  # 데이터가 6개 컬럼과 맞는지 확인
                        vocab = row[0]
                        query = """
                            INSERT INTO groupVocabularyTable (vocabulary)
                            VALUES (%s)
                        """
                        self.cur.execute(query, (vocab,))
                    else:
                        print(f"Skipping invalid row: {row}")

            # 변경 사항 커밋
            self.conn.commit()


        self.cur.execute("SELECT COUNT(*) FROM bookTable")
        result = self.cur.fetchone()

        if result[0] == 0:
            with open('finalData.csv', mode='r',encoding = "cp949") as file, open('write.csv', mode='r',encoding = "cp949") as desc_file:
                csv_reader = csv.reader(file)
                csv_desc_reader = csv.reader(desc_file)
                
                for  row in csv_reader:
                    desc_row = next(csv_desc_reader)
                    if True:  # 데이터가 6개 컬럼과 맞는지 확인
                        name = row[0][:50]
                        author = row[1][:100]
                        publisher = row[2][:30]
                        year = row[3][:4]
                        image = row[4][:255]
                        isbn = row[5][:13]
                        
                        try:
                            query = """
                                INSERT INTO bookTable (name, author, publisher, year, image, ISBN)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            self.cur.execute(query, (name, author, publisher, year, image, isbn))
                        except Exception as e:
                            print(f"An error occurred while executing the query: {e}")

                    else:
                        print(f"Skipping invalid row: {row}, {desc_row}")

            # 변경 사항 커밋
            self.conn.commit()
            with open('write.csv', mode='r',encoding = "cp949") as file:
                csv_reader = csv.reader(file)
                
                for  row in csv_reader:
                    
                    
                    name = row[0][:50]
                    description = row[2]


                        
                    try:
                        query_check = "SELECT id FROM bookTable WHERE name = %s"
                        self.cur.execute(query_check, (name,))
                        result = self.cur.fetchone()
                        if result:  # name에 해당하는 book이 존재하면
                            book_id = result[0]
                           
                    
                    # id를 사용하여 새로운 데이터를 추가합니다.
                            query_update = """
                                UPDATE bookTable
                                SET description = %s
                                WHERE id = %s
                            """
                            # print(description)
                            # print(book_id)
                            self.cur.execute(query_update, (description, book_id))
                            # print(f"update {book_id}")
                        else:
                            print(f"Book with name '{name}' not found.")
                    except Exception as e:
                        print(f"An error occurred while executing the query: {e}")

                
                self.conn.commit()
                query ="""
DELETE FROM booktable
WHERE ID IN (
    23, 926, 1181, 1947, 2275, 2313, 2328, 2873, 2912, 3348, 
    3500, 3532, 4410, 4413, 4686, 4690, 4987, 5493, 5562, 5771, 
    5953, 6096, 6242, 6778, 6853, 7003, 7302, 7442, 7675, 7690, 
    7811, 8068, 8152, 8207, 8224, 8243, 8314, 8454, 8455, 8477, 
    8598, 8601, 8663, 8819, 8822, 8830, 8842
);
"""
                self.cur.execute(query)
                self.conn.commit()
        self.cur.execute("SELECT COUNT(*) FROM bookKeywordTable")
        result = self.cur.fetchone()

        if result[0] == 0:

            with open('bookKeywordExtract.csv', mode='r',encoding = "utf-8") as file:
                csv_reader = csv.reader(file)
                
                for  row in csv_reader:
                    
                    
                    name = row[0][:50]
                    keyword = row[1]+";"+row[2]+";"+row[3]+";"+row[4]+";"+row[5]

                    # print(keyword)
                        
                    try:
                        query_check = "SELECT id FROM bookTable WHERE name = %s"
                        self.cur.execute(query_check, (name,))
                        result = self.cur.fetchone()
                        if result:  # name에 해당하는 book이 존재하면
                            book_id = result[0]
                            # print(book_id)
                           
                    
                    # id를 사용하여 새로운 데이터를 추가합니다.
                            query = """
                                INSERT INTO bookKeywordTable (bookID, keyword)
                                VALUES (%s, %s)
                                
                                """
                            # print(description)
                            # print(book_id)
                            self.cur.execute(query, (book_id,keyword))
                            # print(f"update {book_id}")
                        else:
                            print(f"Book with name '{name}' not found.")
                    except Exception as e:
                        print(f"An error occurred while executing the query: {e}")

                
            self.conn.commit()
        
        

        # self.cur.execute("SELECT COUNT(*) FROM bookKeyWordTable")
        # result = self.cur.fetchone()

        # if result[0] == 0:
        #     with open('bookKeywordExtract.csv', mode='r',encoding = "utf-8") as file:
        #         csv_reader = csv.reader(file)
        #         count =1
        #         for  row in csv_reader:
        #             if True:  # 데이터가 6개 컬럼과 맞는지 확인
        #                 keyword = row[1]+";"+row[2]+";"+row[3]+";"+row[4]+";"+row[5]

        #                 query = """
        #                     INSERT INTO bookKeywordTable (bookID, keyword)
        #                     VALUES (%s, %s)
        #                 """
        #                 # print(count)
        #                 self.cur.execute(query, (count,keyword))
        #                 count += 1
        #             else:
        #                 print(f"Skipping invalid row: {row}")

        #     # 변경 사항 커밋
        #     self.conn.commit()

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
                return "success"
        except Exception as e:
            print(f"에러 발생: {e}")
            return "fail"
            

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
        if(result == "fail"):
            return jsonify({"result":result}), 400
        return jsonify({"result": result})  # 결과를 JSON 형식으로 반환
    else:
        return jsonify({"error": "쿼리를 입력해주세요."}), 400

if __name__ == '__main__':
    app.run('0.0.0.0', port=5001, debug=True)
