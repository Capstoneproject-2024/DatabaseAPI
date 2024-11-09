import pymysql
from dotenv import load_dotenv
import os
from flask import Flask, jsonify,request
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

        try:
            # 테이블 생성
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
        except Exception as e:
            print(f"Error creating table: {e}")

        self.cur.execute("SELECT COUNT(*) FROM bookTable")
        result = self.cur.fetchone()

        if result[0] == 0:
            # CSV 파일을 읽는 부분을 수정
            with open('finalData.csv', mode='r') as file:
                csv_reader = csv.reader(file)

                for row in csv_reader:
                    if len(row) == 6:  # 데이터가 6개 컬럼과 맞는지 확인
                        # 데이터의 길이가 각 컬럼 크기를 초과하지 않도록 처리
                        name = row[0][:50]
                        author = row[1][:100]
                        publisher = row[2][:30]
                        year = row[3][:4]
                        image = row[4][:255]  # image 컬럼 크기에 맞게 조정 (최대 255자 예시)
                        isbn = row[5][:13]  # ISBN은 13자리로 제한

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